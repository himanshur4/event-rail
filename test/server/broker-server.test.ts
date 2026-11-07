// The client here is deliberately raw, so the server is tested on its own,
// including with bytes the real client would never send. Port 0 makes the
// operating system pick a free port, so tests never clash.

import { after, describe, test } from "node:test";
import assert from "node:assert/strict";
import { connect, type Socket } from "node:net";

import { decodeResponse, encodeRequest } from "../../src/protocol/codec.js";
import { FrameDecoder, encodeFrame } from "../../src/protocol/frame.js";
import type { ApiRequest, ApiResponse } from "../../src/protocol/messages.js";
import { BrokerServer, type BrokerServerOptions } from "../../src/server/broker-server.js";
import { TopicManager } from "../../src/topic/topic-manager.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

after(removeTempDirs);

// A minimal client: send raw bytes, receive whole frames, notice when closed.
class RawClient {
  readonly socket: Socket;
  readonly closed: Promise<void>;
  private readonly decoder = new FrameDecoder();
  private readonly frames: Buffer[] = [];
  // Callers waiting for a frame that hasn't arrived yet.
  private readonly waiting: Array<(frame: Buffer) => void> = [];

  private constructor(socket: Socket) {
    this.socket = socket;
    this.closed = new Promise((resolve) => socket.once("close", () => resolve()));
    socket.on("error", () => {}); // a reset by the server is expected in some tests
    socket.on("data", (chunk: Buffer) => {
      for (const frame of this.decoder.push(chunk)) {
        const waiter = this.waiting.shift();
        if (waiter === undefined) {
          this.frames.push(frame);
        } else {
          waiter(frame);
        }
      }
    });
  }

  static connect(port: number): Promise<RawClient> {
    return new Promise((resolve, reject) => {
      const socket = connect({ host: "127.0.0.1", port });
      socket.once("error", reject);
      socket.once("connect", () => resolve(new RawClient(socket)));
    });
  }

  /** Sends a request with the given correlation ID, framed. */
  send(correlationId: number, request: ApiRequest): void {
    this.socket.write(encodeFrame(encodeRequest(correlationId, request)));
  }

  /** The next frame from the server, decoded as a response. */
  async response(): Promise<{ correlationId: number; response: ApiResponse }> {
    const frame = this.frames.shift() ?? (await new Promise<Buffer>((resolve) => this.waiting.push(resolve)));
    return { correlationId: frame.readUInt32BE(4), response: decodeResponse(frame) };
  }
}

// Starts a broker with a fresh data directory, runs `body`, then shuts
// everything down in the right order: server first, storage last.
async function withBroker(
  options: BrokerServerOptions,
  body: (server: BrokerServer, logs: string[]) => Promise<void>,
): Promise<void> {
  const topics = await TopicManager.open(await createTempDir(), { logOptions: { syncOnAppend: false } });
  const logs: string[] = [];
  const server = await BrokerServer.start(topics, { port: 0, ...options, log: (line) => logs.push(line) });
  try {
    await body(server, logs);
  } finally {
    await server.close();
    await topics.close();
  }
}

describe("BrokerServer", () => {
  test("answers a request over TCP", async () => {
    await withBroker({}, async (server) => {
      const client = await RawClient.connect(server.port);
      client.send(7, { type: "createTopic", topic: "orders", partitionCount: 2 });
      assert.deepEqual(await client.response(), { correlationId: 7, response: { type: "createTopic" } });
      client.send(8, { type: "metadata" });
      assert.deepEqual(await client.response(), {
        correlationId: 8,
        response: { type: "metadata", topics: [{ name: "orders", partitionCount: 2 }] },
      });
      client.socket.end();
      await client.closed;
    });
  });

  test("requests sent together on one connection run and are answered in order", async () => {
    await withBroker({}, async (server) => {
      const client = await RawClient.connect(server.port);
      // All five requests leave in one write, without waiting for any answer.
      const requests: ApiRequest[] = [
        { type: "createTopic", topic: "orders", partitionCount: 1 },
        { type: "produce", topic: "orders", partition: 0, records: [{ key: null, value: Buffer.from("A") }] },
        { type: "produce", topic: "orders", partition: 0, records: [{ key: null, value: Buffer.from("B") }] },
        { type: "produce", topic: "orders", partition: 0, records: [{ key: null, value: Buffer.from("C") }] },
        { type: "fetch", topic: "orders", partition: 0, offset: 0, maxRecords: 10 },
      ];
      client.socket.write(Buffer.concat(requests.map((request, i) => encodeFrame(encodeRequest(i, request)))));

      const answers = [];
      for (let i = 0; i < requests.length; i++) {
        answers.push(await client.response());
      }
      assert.deepEqual(answers.map((a) => a.correlationId), [0, 1, 2, 3, 4]);
      const fetched = answers[4]!.response;
      assert.equal(fetched.type, "fetch");
      if (fetched.type === "fetch") {
        // A was stored before B before C: the order they were sent.
        assert.deepEqual(fetched.records.map((r) => r.value.toString()), ["A", "B", "C"]);
      }
      client.socket.end();
    });
  });

  test("a failed request gets an error response and the connection stays usable", async () => {
    await withBroker({}, async (server) => {
      const client = await RawClient.connect(server.port);
      client.send(1, { type: "fetch", topic: "missing", partition: 0, offset: 0, maxRecords: 1 });
      assert.deepEqual(await client.response(), {
        correlationId: 1,
        response: { type: "error", code: "UNKNOWN_TOPIC", message: "topic missing does not exist" },
      });

      // An API this broker doesn't know: header apiKey 99, version 0, correlation ID 2.
      client.socket.write(encodeFrame(Buffer.from([0, 99, 0, 0, 0, 0, 0, 2])));
      const unsupported = await client.response();
      assert.equal(unsupported.correlationId, 2);
      assert.equal(unsupported.response.type === "error" && unsupported.response.code, "UNSUPPORTED_VERSION");

      // A valid header with a body that is cut short: metadata header + 1 stray byte.
      client.socket.write(encodeFrame(Buffer.from([0, 1, 0, 0, 0, 0, 0, 3, 0xff])));
      const malformed = await client.response();
      assert.equal(malformed.response.type === "error" && malformed.response.code, "MALFORMED_MESSAGE");

      // Still the same connection, still working.
      client.send(4, { type: "metadata" });
      assert.deepEqual(await client.response(), { correlationId: 4, response: { type: "metadata", topics: [] } });
      client.socket.end();
    });
  });

  test("a frame too short for a header closes the connection, and only that one", async () => {
    await withBroker({}, async (server, logs) => {
      const bad = await RawClient.connect(server.port);
      const good = await RawClient.connect(server.port);
      bad.socket.write(encodeFrame(Buffer.from([0, 1, 0]))); // 3 bytes: no correlation ID to answer
      await bad.closed;
      assert.ok(logs.some((line) => line.includes("too short for a header")));
      good.send(1, { type: "metadata" });
      assert.equal((await good.response()).response.type, "metadata");
      good.socket.end();
    });
  });

  test("a frame over the size limit closes the connection as soon as its length arrives", async () => {
    // The record limit must shrink with the frame limit, or the broker refuses
    // to start (a maximum-size record could never be fetched).
    await withBroker({ maxFrameBytes: 1024, maxRecordBytes: 512 }, async (server) => {
      const client = await RawClient.connect(server.port);
      // Only the 4-byte length, announcing 1025 bytes: no payload is ever sent.
      client.socket.write(Buffer.from([0, 0, 0x04, 0x01]));
      await client.closed;
    });
  });

  test("a client that leaves mid-request doesn't affect the broker; its partial request never runs", async () => {
    await withBroker({}, async (server, logs) => {
      const leaver = await RawClient.connect(server.port);
      const frame = encodeFrame(encodeRequest(1, { type: "createTopic", topic: "half", partitionCount: 1 }));
      leaver.socket.end(frame.subarray(0, frame.length - 2)); // all but the last 2 bytes, then leave
      await leaver.closed;

      const client = await RawClient.connect(server.port);
      client.send(1, { type: "metadata" });
      // The topic was never created: its request was never complete.
      assert.deepEqual((await client.response()).response, { type: "metadata", topics: [] });
      assert.ok(logs.some((line) => line.includes("in the middle of a request")));
      client.socket.end();
    });
  });

  test("a client that says it has finished sending still gets its answers", async () => {
    await withBroker({}, async (server) => {
      const client = await RawClient.connect(server.port);
      client.send(1, { type: "createTopic", topic: "orders", partitionCount: 1 });
      client.send(2, { type: "metadata" });
      client.socket.end(); // "no more requests from me"
      assert.equal((await client.response()).correlationId, 1);
      assert.equal((await client.response()).correlationId, 2);
      await client.closed; // then the broker closes too
    });
  });

  test("connections over the limit are refused; existing ones keep working", async () => {
    await withBroker({ maxConnections: 1 }, async (server, logs) => {
      const first = await RawClient.connect(server.port);
      const second = await RawClient.connect(server.port);
      await second.closed;
      assert.ok(logs.some((line) => line.includes("too many connections")));
      first.send(1, { type: "metadata" });
      assert.equal((await first.response()).response.type, "metadata");
      first.socket.end();
    });
  });

  test("an idle connection is closed after the idle timeout", async () => {
    await withBroker({ idleTimeoutMs: 50 }, async (server, logs) => {
      const client = await RawClient.connect(server.port);
      await client.closed; // nothing sent; the broker gives up on it
      assert.ok(logs.some((line) => line.includes("idle")));
    });
  });

  test("many requests in flight with a small limit are all answered, in order", async () => {
    // maxInFlightRequests 2: the connection pauses reading after 2 waiting
    // requests and resumes as they are answered. Nothing may be lost.
    await withBroker({ maxInFlightRequests: 2 }, async (server) => {
      const client = await RawClient.connect(server.port);
      const count = 50;
      const frames = [];
      for (let i = 0; i < count; i++) {
        frames.push(encodeFrame(encodeRequest(i, { type: "metadata" })));
      }
      client.socket.write(Buffer.concat(frames));
      for (let i = 0; i < count; i++) {
        assert.equal((await client.response()).correlationId, i);
      }
      client.socket.end();
    });
  });

  test("close() answers requests already received, then refuses new connections", async () => {
    // fsync stays ON here: every produce waits for the disk, so most of the
    // requests below are still queued when close() is called.
    const topics = await TopicManager.open(await createTempDir());
    const server = await BrokerServer.start(topics, { port: 0, maxInFlightRequests: 1000 });
    const port = server.port;
    try {
      const client = await RawClient.connect(port);
      client.send(0, { type: "createTopic", topic: "orders", partitionCount: 1 });
      assert.equal((await client.response()).correlationId, 0);

      // 30 produce requests in one write (about 1 KB: one chunk on this computer).
      const count = 30;
      const frames = [];
      for (let id = 1; id <= count; id++) {
        frames.push(
          encodeFrame(encodeRequest(id, { type: "produce", topic: "orders", partition: 0, records: [{ key: null, value: Buffer.from(`r${id}`) }] })),
        );
      }
      client.socket.write(Buffer.concat(frames));
      // The first answer proves the broker has received the requests; it is
      // now working through the rest when shutdown begins.
      assert.equal((await client.response()).correlationId, 1);
      const closing = server.close();
      for (let id = 2; id <= count; id++) {
        assert.equal((await client.response()).correlationId, id, "a received request was not answered");
      }
      await client.closed; // the broker ended the connection after answering
      await closing;
      assert.equal(server.connectionCount, 0);
      await assert.rejects(RawClient.connect(port)); // nobody listening any more
    } finally {
      await server.close(); // safe to call twice
      await topics.close();
    }
  });
});
