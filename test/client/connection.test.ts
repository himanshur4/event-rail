// Tests the client against a fake broker whose answers each test controls, so it
// can be late, wrong, or silent on purpose.

import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { createServer, type AddressInfo, type Server, type Socket } from "node:net";

import { BrokerError, ConnectionError } from "../../src/client/client-errors.js";
import { Connection } from "../../src/client/connection.js";
import { decodeHeader, encodeResponse } from "../../src/protocol/codec.js";
import { FrameDecoder, encodeFrame } from "../../src/protocol/frame.js";
import type { MessageHeader } from "../../src/protocol/messages.js";
import { ProtocolError } from "../../src/protocol/protocol-error.js";

// A fake broker: records the header of every request it receives, and lets the
// test decide what to send back, and when.
class FakeBroker {
  readonly server: Server;
  readonly headers: MessageHeader[] = [];
  socket: Socket | null = null;
  // Runs for each request; the test sets it to answer (or not).
  onRequest: (header: MessageHeader) => void = () => {};

  constructor() {
    // allowHalfOpen, like the real broker: when the client ends its side, the
    // fake can still send answers it owes.
    this.server = createServer({ allowHalfOpen: true }, (socket) => {
      this.socket = socket;
      socket.on("error", () => {});
      const decoder = new FrameDecoder();
      socket.on("data", (chunk: Buffer) => {
        for (const frame of decoder.push(chunk)) {
          const header = decodeHeader(frame);
          this.headers.push(header);
          this.onRequest(header);
        }
      });
    });
  }

  async start(): Promise<number> {
    await new Promise<void>((resolve) => this.server.listen(0, "127.0.0.1", () => resolve()));
    return (this.server.address() as AddressInfo).port;
  }

  /** Sends raw payload bytes, framed, to the connected client. */
  reply(payload: Buffer): void {
    this.socket!.write(encodeFrame(payload));
  }

  async stop(): Promise<void> {
    this.socket?.destroy();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

// Runs `body` with a fake broker and a client Connection to it.
async function withFake(
  requestTimeoutMs: number,
  body: (fake: FakeBroker, connection: Connection) => Promise<void>,
): Promise<void> {
  const fake = new FakeBroker();
  const port = await fake.start();
  const connection = await Connection.connect({ port, requestTimeoutMs });
  try {
    await body(fake, connection);
  } finally {
    await fake.stop();
    await connection.closed;
  }
}

function isConnectionError(code: string): (err: unknown) => boolean {
  return (err: unknown) => err instanceof ConnectionError && err.code === code;
}

describe("Connection", () => {
  test("answers are matched by correlation ID, even when they arrive in a different order", async () => {
    await withFake(5000, async (fake, connection) => {
      // Answer only once both requests have arrived, second one first.
      fake.onRequest = () => {
        if (fake.headers.length === 2) {
          const [first, second] = fake.headers;
          fake.reply(encodeResponse(second!, { type: "produce", baseOffset: 5, lastOffset: 5 }));
          fake.reply(encodeResponse(first!, { type: "metadata", topics: [{ name: "first", partitionCount: 1 }] }));
        }
      };
      const metadata = connection.send({ type: "metadata" });
      const produce = connection.send({ type: "produce", topic: "t", partition: 0, records: [{ key: null, value: Buffer.from("x") }] });
      assert.deepEqual(await metadata, { type: "metadata", topics: [{ name: "first", partitionCount: 1 }] });
      assert.deepEqual(await produce, { type: "produce", baseOffset: 5, lastOffset: 5 });
      // Two requests, two different correlation IDs.
      assert.notEqual(fake.headers[0]!.correlationId, fake.headers[1]!.correlationId);
    });
  });

  test("an error response rejects only its own request, with BrokerError", async () => {
    await withFake(5000, async (fake, connection) => {
      fake.onRequest = (header) => {
        const response =
          header.apiKey === 3
            ? ({ type: "error", code: "UNKNOWN_TOPIC", message: "topic t does not exist" } as const)
            : ({ type: "metadata", topics: [] } as const);
        fake.reply(encodeResponse(header, response));
      };
      await assert.rejects(connection.send({ type: "fetch", topic: "t", partition: 0, offset: 0, maxRecords: 1 }), (err: unknown) => {
        return err instanceof BrokerError && err.code === "UNKNOWN_TOPIC";
      });
      assert.deepEqual(await connection.send({ type: "metadata" }), { type: "metadata", topics: [] }); // still fine
    });
  });

  test("a late answer times out its request; when it finally arrives it's ignored, and the connection still works", async () => {
    await withFake(50, async (fake, connection) => {
      fake.onRequest = () => {}; // the first request gets no answer (yet)
      await assert.rejects(connection.send({ type: "metadata" }), isConnectionError("REQUEST_TIMEOUT"));

      // Now answer every request at once, starting with the late answer to the first.
      fake.onRequest = (header) => fake.reply(encodeResponse(header, { type: "createTopic" }));
      fake.reply(encodeResponse(fake.headers[0]!, { type: "metadata", topics: [{ name: "late", partitionCount: 1 }] }));
      // The late metadata answer must NOT be taken as this request's answer.
      assert.deepEqual(await connection.send({ type: "createTopic", topic: "t", partitionCount: 1 }), { type: "createTopic" });
    });
  });

  test("an answer for the wrong API is a protocol violation that fails every waiting request", async () => {
    await withFake(5000, async (fake, connection) => {
      fake.onRequest = (header) => {
        if (fake.headers.length === 2) {
          // Answer the first request (a metadata) as if it were a produce.
          fake.reply(encodeResponse({ ...header, apiKey: 2, correlationId: fake.headers[0]!.correlationId }, { type: "produce", baseOffset: 0, lastOffset: 0 }));
        }
      };
      const first = connection.send({ type: "metadata" });
      const second = connection.send({ type: "metadata" });
      await assert.rejects(first, isConnectionError("PROTOCOL_VIOLATION"));
      await assert.rejects(second, isConnectionError("PROTOCOL_VIOLATION"));
      await assert.rejects(connection.send({ type: "metadata" }), isConnectionError("PROTOCOL_VIOLATION"));
    });
  });

  test("bytes that don't decode are a protocol violation", async () => {
    await withFake(5000, async (fake, connection) => {
      fake.onRequest = (header) => {
        const good = encodeResponse(header, { type: "metadata", topics: [] });
        fake.reply(Buffer.concat([good, Buffer.from([0xff])])); // one stray byte at the end
      };
      await assert.rejects(connection.send({ type: "metadata" }), isConnectionError("PROTOCOL_VIOLATION"));
    });
  });

  test("if the connection drops, every waiting request fails with CONNECTION_CLOSED", async () => {
    await withFake(5000, async (fake, connection) => {
      fake.onRequest = () => {
        if (fake.headers.length === 2) {
          fake.socket!.destroy(); // the broker vanishes without answering
        }
      };
      const first = connection.send({ type: "metadata" });
      const second = connection.send({ type: "metadata" });
      await assert.rejects(first, isConnectionError("CONNECTION_CLOSED"));
      await assert.rejects(second, isConnectionError("CONNECTION_CLOSED"));
    });
  });

  test("a request too large to send fails on its own; the connection stays usable", async () => {
    const fake = new FakeBroker();
    const port = await fake.start();
    const connection = await Connection.connect({ port, maxFrameBytes: 64 });
    try {
      fake.onRequest = (header) => fake.reply(encodeResponse(header, { type: "metadata", topics: [] }));
      await assert.rejects(
        connection.send({ type: "produce", topic: "t", partition: 0, records: [{ key: null, value: Buffer.alloc(100) }] }),
        (err: unknown) => err instanceof ProtocolError && err.code === "FRAME_TOO_LARGE",
      );
      assert.equal(fake.headers.length, 0); // nothing was sent
      assert.deepEqual(await connection.send({ type: "metadata" }), { type: "metadata", topics: [] });
    } finally {
      await fake.stop();
      await connection.closed;
    }
  });

  test("after close(), new requests are refused, but a request already sent still gets its answer", async () => {
    await withFake(5000, async (fake, connection) => {
      fake.onRequest = (header) => {
        // Answer a moment later, after the client has called close().
        setTimeout(() => {
          fake.reply(encodeResponse(header, { type: "metadata", topics: [] }));
          fake.socket!.end();
        }, 20);
      };
      const inFlight = connection.send({ type: "metadata" });
      const closing = connection.close();
      await assert.rejects(connection.send({ type: "metadata" }), isConnectionError("CONNECTION_CLOSED"));
      assert.deepEqual(await inFlight, { type: "metadata", topics: [] });
      await closing;
    });
  });
});
