import { after, describe, test } from "node:test";
import assert from "node:assert/strict";

import { RequestHandler, errorResponseFor, type RequestHandlerOptions } from "../../src/broker/request-handler.js";
import { RequestError } from "../../src/broker/request-error.js";
import { encodeResponse } from "../../src/protocol/codec.js";
import { API_KEYS, API_VERSION, type FetchResponse } from "../../src/protocol/messages.js";
import { LogError } from "../../src/storage/log.js";
import { TopicManager } from "../../src/topic/topic-manager.js";
import { TopicError } from "../../src/topic/topic-metadata.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

after(removeTempDirs);

// Opens a TopicManager in a fresh directory, runs `body` with a handler for it,
// and always closes the manager afterwards (Windows can't delete open files).
// syncOnAppend: false skips fsync, which only makes these tests slower.
async function withHandler(
  options: RequestHandlerOptions,
  body: (handler: RequestHandler, topics: TopicManager) => Promise<void>,
): Promise<void> {
  const topics = await TopicManager.open(await createTempDir(), { logOptions: { syncOnAppend: false } });
  try {
    await body(new RequestHandler(topics, options), topics);
  } finally {
    await topics.close();
  }
}

// A record whose key + value total `size` bytes (no key, so all value).
function recordOfSize(size: number): { key: null; value: Buffer } {
  return { key: null, value: Buffer.alloc(size, 0x61) };
}

// The encoded length of a fetch response, as the server would send it.
function encodedLength(response: FetchResponse): number {
  return encodeResponse({ apiKey: API_KEYS.fetch, apiVersion: API_VERSION, correlationId: 1 }, response).length;
}

describe("RequestHandler", () => {
  test("createTopic, then metadata lists the topics, sorted by name", async () => {
    await withHandler({}, async (handler) => {
      assert.deepEqual(await handler.handle({ type: "createTopic", topic: "payments", partitionCount: 1 }), {
        type: "createTopic",
      });
      await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 3 });
      assert.deepEqual(await handler.handle({ type: "metadata" }), {
        type: "metadata",
        topics: [
          { name: "orders", partitionCount: 3 },
          { name: "payments", partitionCount: 1 },
        ],
      });
    });
  });

  test("produce stores records and returns their offsets; fetch reads them back", async () => {
    await withHandler({}, async (handler) => {
      await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 2 });
      const records = [
        { key: Buffer.from("user-42"), value: Buffer.from("order placed") },
        { key: null, value: Buffer.from("order paid") },
      ];
      assert.deepEqual(await handler.handle({ type: "produce", topic: "orders", partition: 1, records }), {
        type: "produce",
        baseOffset: 0,
        lastOffset: 1,
      });

      const fetched = await handler.handle({ type: "fetch", topic: "orders", partition: 1, offset: 0, maxRecords: 10 });
      assert.equal(fetched.type, "fetch");
      if (fetched.type === "fetch") {
        assert.equal(fetched.logEndOffset, 2);
        assert.deepEqual(
          fetched.records.map((r) => [r.offset, r.key?.toString() ?? null, r.value.toString()]),
          [
            [0, "user-42", "order placed"],
            [1, null, "order paid"],
          ],
        );
      }
      // At the end of the log: no records, and the end offset says "caught up".
      assert.deepEqual(await handler.handle({ type: "fetch", topic: "orders", partition: 1, offset: 2, maxRecords: 10 }), {
        type: "fetch",
        logEndOffset: 2,
        records: [],
      });
    });
  });

  test("storage and topic errors are thrown as their own error classes", async () => {
    await withHandler({}, async (handler) => {
      await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 1 });
      const cases: Array<[Promise<unknown>, string]> = [
        [handler.handle({ type: "createTopic", topic: "orders", partitionCount: 1 }), "TOPIC_EXISTS"],
        [handler.handle({ type: "createTopic", topic: "Bad Name", partitionCount: 1 }), "INVALID_TOPIC_NAME"],
        [handler.handle({ type: "produce", topic: "missing", partition: 0, records: [recordOfSize(1)] }), "UNKNOWN_TOPIC"],
        [handler.handle({ type: "produce", topic: "orders", partition: 5, records: [recordOfSize(1)] }), "UNKNOWN_PARTITION"],
        [handler.handle({ type: "produce", topic: "orders", partition: 0, records: [] }), "EMPTY_BATCH"],
        [handler.handle({ type: "fetch", topic: "orders", partition: 0, offset: 9, maxRecords: 1 }), "OFFSET_OUT_OF_RANGE"],
      ];
      for (const [promise, code] of cases) {
        await assert.rejects(promise, (err: unknown) => {
          assert.ok(err instanceof TopicError || err instanceof LogError, `${code}: wrong error class`);
          assert.equal(err.code, code);
          return true;
        });
      }
    });
  });

  test("fetch with maxRecords 0 is an error, not an empty 'caught up' answer", async () => {
    await withHandler({}, async (handler) => {
      await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 1 });
      await handler.handle({ type: "produce", topic: "orders", partition: 0, records: [recordOfSize(1)] });
      await assert.rejects(
        handler.handle({ type: "fetch", topic: "orders", partition: 0, offset: 0, maxRecords: 0 }),
        (err: unknown) => err instanceof LogError && err.code === "INVALID_MAX_RECORDS",
      );
    });
  });

  test("a record over the size limit is refused, and none of its batch is stored", async () => {
    await withHandler({ maxRecordBytes: 100 }, async (handler, topics) => {
      await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 1 });
      await assert.rejects(
        handler.handle({ type: "produce", topic: "orders", partition: 0, records: [recordOfSize(100), recordOfSize(101)] }),
        (err: unknown) => err instanceof RequestError && err.code === "RECORD_TOO_LARGE",
      );
      assert.equal(topics.partition("orders", 0).logEndOffset, 0); // nothing appended
      // Exactly the limit is accepted; the key counts towards it too.
      await handler.handle({ type: "produce", topic: "orders", partition: 0, records: [recordOfSize(100)] });
      await assert.rejects(
        handler.handle({
          type: "produce",
          topic: "orders",
          partition: 0,
          records: [{ key: Buffer.alloc(1), value: Buffer.alloc(100) }],
        }),
        (err: unknown) => err instanceof RequestError,
      );
    });
  });

  test("a fetch response stays within the response limit, but always holds at least one record", async () => {
    // 22 bytes of response fields + 24 bytes of record fields + 100 = 146:
    // the smallest response limit that can carry a maximum-size record.
    for (const maxResponseBytes of [146, 300, 1000]) {
      await withHandler({ maxRecordBytes: 100, maxResponseBytes }, async (handler) => {
        await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 1 });
        const sizes = [100, 60, 60, 60, 100, 1, 0, 100];
        await handler.handle({ type: "produce", topic: "orders", partition: 0, records: sizes.map(recordOfSize) });

        // Fetch repeatedly from where the last response ended, like a consumer.
        let offset = 0;
        const seen: number[] = [];
        while (offset < sizes.length) {
          const response = await handler.handle({ type: "fetch", topic: "orders", partition: 0, offset, maxRecords: 100 });
          assert.equal(response.type, "fetch");
          if (response.type === "fetch") {
            assert.ok(response.records.length >= 1, "a fetch must make progress");
            assert.ok(encodedLength(response) <= maxResponseBytes, `response over ${maxResponseBytes} bytes`);
            for (const record of response.records) {
              seen.push(record.value.length);
            }
            offset = response.records.at(-1)!.offset + 1;
          }
        }
        assert.deepEqual(seen, sizes); // every record, in order, none skipped
      });
    }
  });

  test("a fetch can read more records than one read chunk, up to maxRecords", async () => {
    // With the defaults, the handler reads 7 records from the log at a time;
    // 20 small records need several reads, and maxRecords must still be honoured.
    await withHandler({}, async (handler) => {
      await handler.handle({ type: "createTopic", topic: "orders", partitionCount: 1 });
      const records = Array.from({ length: 20 }, () => recordOfSize(5));
      await handler.handle({ type: "produce", topic: "orders", partition: 0, records });
      for (const [maxRecords, expected] of [[20, 20], [13, 13], [50, 20]]) {
        const response = await handler.handle({ type: "fetch", topic: "orders", partition: 0, offset: 0, maxRecords: maxRecords! });
        assert.equal(response.type === "fetch" ? response.records.length : -1, expected);
      }
    });
  });

  test("limits that can't fetch a maximum-size record are refused", async () => {
    await withHandler({}, async (_handler, topics) => {
      assert.throws(() => new RequestHandler(topics, { maxRecordBytes: 100, maxResponseBytes: 145 }), RangeError);
    });
  });
});

describe("errorResponseFor", () => {
  test("the broker's own errors keep their code and message", () => {
    assert.deepEqual(errorResponseFor(new TopicError("UNKNOWN_TOPIC", "topic t does not exist")), {
      type: "error",
      code: "UNKNOWN_TOPIC",
      message: "topic t does not exist",
    });
    assert.equal(errorResponseFor(new RequestError("RECORD_TOO_LARGE", "x")).code, "RECORD_TOO_LARGE");
  });

  test("any other error becomes UNKNOWN_SERVER_ERROR, without its details", () => {
    assert.deepEqual(errorResponseFor(new Error("EACCES: C:\\secret\\path")), {
      type: "error",
      code: "UNKNOWN_SERVER_ERROR",
      message: "unexpected broker error",
    });
  });
});
