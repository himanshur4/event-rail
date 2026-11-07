// End-to-end tests: the clients against a real broker, over real TCP, storing to
// real files.

import { after, describe, test } from "node:test";
import assert from "node:assert/strict";

import { Admin } from "../../src/client/admin.js";
import { BrokerError, ConnectionError } from "../../src/client/client-errors.js";
import { Connection } from "../../src/client/connection.js";
import { Consumer } from "../../src/client/consumer.js";
import { Producer } from "../../src/client/producer.js";
import type { LogRecord, RecordInput } from "../../src/common/types.js";
import { BrokerServer } from "../../src/server/broker-server.js";
import { partitionForKey } from "../../src/topic/partitioner.js";
import { TopicManager } from "../../src/topic/topic-manager.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

after(removeTempDirs);

// Starts a broker (port 0 = any free port) with fresh storage, opens one
// client connection to it, runs `body`, then closes everything in order.
async function withClient(
  body: (connection: Connection, server: BrokerServer, dataDir: string) => Promise<void>,
  maxRecordBytes?: number,
): Promise<void> {
  const dataDir = await createTempDir();
  const topics = await TopicManager.open(dataDir, { logOptions: { syncOnAppend: false } });
  const server = await BrokerServer.start(topics, { port: 0, ...(maxRecordBytes === undefined ? {} : { maxRecordBytes }) });
  const connection = await Connection.connect({ port: server.port });
  try {
    await body(connection, server, dataDir);
  } finally {
    await connection.close().catch(() => {}); // may already be closed by the test
    await server.close();
    await topics.close();
  }
}

// A record from text, for readability.
function record(key: string | null, value: string): RecordInput {
  return { key: key === null ? null : Buffer.from(key), value: Buffer.from(value) };
}

// Polls until caught up and returns every record's value as text.
async function readAll(consumer: Consumer): Promise<string[]> {
  const values: string[] = [];
  for (;;) {
    const records: readonly LogRecord[] = await consumer.poll();
    if (records.length === 0) {
      return values;
    }
    values.push(...records.map((r) => r.value.toString()));
  }
}

describe("clients against a real broker", () => {
  test("Admin creates and lists topics; broker errors arrive as BrokerError", async () => {
    await withClient(async (connection) => {
      const admin = new Admin(connection);
      await admin.createTopic("orders", 3);
      await admin.createTopic("audit", 1);
      assert.deepEqual(await admin.listTopics(), [
        { name: "audit", partitionCount: 1 },
        { name: "orders", partitionCount: 3 },
      ]);
      await assert.rejects(admin.createTopic("orders", 3), (err: unknown) => {
        assert.ok(err instanceof BrokerError);
        assert.equal(err.code, "TOPIC_EXISTS");
        assert.equal(err.message, "topic orders already exists"); // the broker's own message
        return true;
      });
    });
  });

  test("the producer sends each key to its partition; consumers read every partition back in order", async () => {
    await withClient(async (connection) => {
      await new Admin(connection).createTopic("orders", 3);
      const producer = new Producer(connection);
      // These two keys were chosen because they hash to different partitions
      // (user-4 -> 0, user-5 -> 1, by the partitioner's MurmurHash3). Checked here,
      // so the test can't silently stop testing two partitions.
      const pA = partitionForKey(Buffer.from("user-4"), 3);
      const pB = partitionForKey(Buffer.from("user-5"), 3);
      assert.notEqual(pA, pB);

      const results = await producer.send("orders", [
        record("user-4", "4: placed"),
        record("user-5", "5: placed"),
        record("user-4", "4: paid"),
        record("user-5", "5: paid"),
        record("user-4", "4: shipped"),
      ]);

      // Same key -> same partition, every time; offsets count up within each partition.
      assert.deepEqual(results, [
        { partition: pA, offset: 0 },
        { partition: pB, offset: 0 },
        { partition: pA, offset: 1 },
        { partition: pB, offset: 1 },
        { partition: pA, offset: 2 },
      ]);
      // Each user's events are in one partition, in the order they were sent.
      assert.deepEqual(await readAll(new Consumer(connection, "orders", pA)), ["4: placed", "4: paid", "4: shipped"]);
      assert.deepEqual(await readAll(new Consumer(connection, "orders", pB)), ["5: placed", "5: paid"]);
      // A second send continues where the first left off.
      assert.deepEqual(await producer.send("orders", [record("user-4", "4: delivered")]), [{ partition: pA, offset: 3 }]);
    });
  });

  test("records without a key are spread round-robin over the partitions", async () => {
    await withClient(async (connection) => {
      await new Admin(connection).createTopic("events", 3);
      const results = await new Producer(connection).send("events", ["a", "b", "c", "d", "e", "f"].map((v) => record(null, v)));
      assert.deepEqual(results.map((r) => [r.partition, r.offset]), [[0, 0], [1, 0], [2, 0], [0, 1], [1, 1], [2, 1]]);
      assert.deepEqual(await readAll(new Consumer(connection, "events", 1)), ["b", "e"]);
    });
  });

  test("producer errors: an unknown topic, and a record over the size limit", async () => {
    await withClient(async (connection) => {
      const producer = new Producer(connection);
      await assert.rejects(producer.send("missing", [record(null, "x")]), (err: unknown) => {
        return err instanceof BrokerError && err.code === "UNKNOWN_TOPIC";
      });
      await new Admin(connection).createTopic("small", 1);
      await assert.rejects(producer.send("small", [record(null, "x".repeat(101))]), (err: unknown) => {
        return err instanceof BrokerError && err.code === "RECORD_TOO_LARGE";
      });
      assert.deepEqual(await producer.send("small", [record(null, "x".repeat(100))]), [{ partition: 0, offset: 0 }]);
    }, 100);
  });

  test("a consumer tracks its position and lag, and can seek", async () => {
    await withClient(async (connection) => {
      await new Admin(connection).createTopic("orders", 1);
      await new Producer(connection).send("orders", ["a", "b", "c", "d"].map((v) => record(null, v)));

      const consumer = new Consumer(connection, "orders", 0, { maxRecords: 3 });
      assert.equal(consumer.lag, null); // unknown before the first poll
      assert.deepEqual((await consumer.poll()).map((r) => r.value.toString()), ["a", "b", "c"]);
      assert.equal(consumer.position, 3);
      assert.equal(consumer.lag, 1);
      assert.deepEqual((await consumer.poll()).map((r) => r.value.toString()), ["d"]);
      assert.deepEqual(await consumer.poll(), []); // caught up
      assert.equal(consumer.lag, 0);

      consumer.seek(1); // re-read from offset 1
      assert.deepEqual((await consumer.poll()).map((r) => r.offset), [1, 2, 3]);
      consumer.seek(99); // past the end: the broker refuses
      await assert.rejects(consumer.poll(), (err: unknown) => err instanceof BrokerError && err.code === "OFFSET_OUT_OF_RANGE");
    });
  });

  test("many requests in flight on one connection each get their own answer", async () => {
    await withClient(async (connection) => {
      await new Admin(connection).createTopic("orders", 4);
      // 40 requests of three kinds, all started before any answer arrives.
      const requests = [];
      for (let i = 0; i < 40; i++) {
        if (i % 3 === 0) {
          requests.push(connection.send({ type: "metadata" }).then((r) => `metadata ${r.topics.length}`));
        } else if (i % 3 === 1) {
          requests.push(
            connection
              .send({ type: "produce", topic: "orders", partition: i % 4, records: [record(null, `v${i}`)] })
              .then((r) => `produce ${r.type}`),
          );
        } else {
          requests.push(connection.send({ type: "fetch", topic: "nope", partition: 0, offset: 0, maxRecords: 1 }).catch((e: BrokerError) => `fetch ${e.code}`));
        }
      }
      const answers = await Promise.all(requests);
      answers.forEach((answer, i) => {
        const expected = ["metadata 1", "produce produce", "fetch UNKNOWN_TOPIC"][i % 3];
        assert.equal(answer, expected, `request ${i}`);
      });
    });
  });

  test("records survive a broker restart", async () => {
    // The whole stack, twice: write through one broker, stop it, start another
    // on the same data directory, read through the new one.
    const dataDir = await createTempDir();
    for (const step of ["write", "read"]) {
      const topics = await TopicManager.open(dataDir);
      const server = await BrokerServer.start(topics, { port: 0 });
      const connection = await Connection.connect({ port: server.port });
      try {
        if (step === "write") {
          await new Admin(connection).createTopic("orders", 1);
          await new Producer(connection).send("orders", [record("k", "durable")]);
        } else {
          assert.deepEqual(await readAll(new Consumer(connection, "orders", 0)), ["durable"]);
        }
      } finally {
        await connection.close();
        await server.close();
        await topics.close();
      }
    }
  });

  test("when the broker shuts down, the client's later requests fail with CONNECTION_CLOSED", async () => {
    await withClient(async (connection, server) => {
      await server.close();
      await connection.closed; // the broker ended the connection
      await assert.rejects(connection.send({ type: "metadata" }), (err: unknown) => {
        return err instanceof ConnectionError && err.code === "CONNECTION_CLOSED";
      });
    });
  });

  test("connecting to a port where nothing listens is rejected", async () => {
    // Start and stop a broker to find a port that is free right now.
    const topics = await TopicManager.open(await createTempDir());
    const server = await BrokerServer.start(topics, { port: 0 });
    const port = server.port;
    await server.close();
    await topics.close();
    await assert.rejects(Connection.connect({ port }));
  });
});
