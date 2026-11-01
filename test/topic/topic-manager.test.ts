// Naming rules, topic creation and on-disk layout, per-key ordering across
// partitions, restarts, damaged topics, and closing.

import { after, afterEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { appendFile, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import type { RecordInput } from "../../src/common/types.js";
import { encodeBatch } from "../../src/storage/record-batch.js";
import { Partitioner } from "../../src/topic/partitioner.js";
import { TopicManager } from "../../src/topic/topic-manager.js";
import {
  MAX_PARTITIONS,
  TopicError,
  topicNameProblem,
  type TopicErrorCode,
} from "../../src/topic/topic-metadata.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

// The first segment file of every partition's log.
const FIRST_LOG = "00000000000000000000.log";

after(removeTempDirs);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Every manager a test opens is closed after it, even if the test failed: on
// Windows a directory can't be deleted while a file in it is open.
const openManagers: TopicManager[] = [];
async function openManager(dataDir: string): Promise<TopicManager> {
  const manager = await TopicManager.open(dataDir, { logOptions: { clock: () => 1_000 } });
  openManagers.push(manager);
  return manager;
}
afterEach(async () => {
  for (const manager of openManagers.splice(0)) {
    await manager.close();
  }
});

function record(value: string, key: string | null = null): RecordInput {
  return { key: key === null ? null : Buffer.from(key), value: Buffer.from(value) };
}

// Accepts only a TopicError with the given code.
function isTopicError(code: TopicErrorCode): (err: unknown) => boolean {
  return (err: unknown) => {
    assert.ok(err instanceof TopicError, "expected a TopicError");
    assert.equal(err.code, code);
    return true;
  };
}

function assertThrowsTopicError(run: () => unknown, code: TopicErrorCode): void {
  assert.throws(run, isTopicError(code));
}

async function assertRejectsTopicError(promise: Promise<unknown>, code: TopicErrorCode): Promise<void> {
  await assert.rejects(promise, isTopicError(code));
}

function valuesOf(records: ReadonlyArray<{ value: Buffer }>): string[] {
  return records.map((r) => r.value.toString());
}

// ---------------------------------------------------------------------------
// Naming rules
// ---------------------------------------------------------------------------

describe("topic names", () => {
  test("accepts lowercase names made of letters, digits, '.', '_' and '-'", () => {
    for (const name of ["orders", "user.events", "a-b_c.1", "0", "x".repeat(100)]) {
      assert.equal(topicNameProblem(name), null, name);
    }
  });

  test("rejects names that would be unsafe or collide as folder names", () => {
    const unsafe = [
      "", // empty
      ".", // "this folder"
      "..", // "the parent folder"
      "../escape", // path traversal
      "a/b", // folder separator (Linux/macOS)
      "a\\b", // folder separator (Windows)
      "Orders", // uppercase: same folder as "orders" on Windows and macOS
      "orders.", // trailing dot: Windows drops it, colliding with "orders"
      "_internal", // must start with a letter or digit
      "a b", // space
      "café", // non-ASCII letter
      "con", // Windows device name
      "con.events", // Windows device name with an extension
      "lpt1", // Windows device name
      "x".repeat(101), // too long
    ];
    for (const name of unsafe) {
      assert.notEqual(topicNameProblem(name), null, JSON.stringify(name));
    }
  });
});

// ---------------------------------------------------------------------------
// TopicManager
// ---------------------------------------------------------------------------

describe("TopicManager", () => {
  describe("creating topics", () => {
    test("a new topic has its partition folders and topic.json on disk", async () => {
      const dataDir = await createTempDir();
      const manager = await openManager(dataDir);
      const metadata = await manager.createTopic("orders", 3);

      assert.deepEqual(metadata, { version: 1, name: "orders", partitionCount: 3 });
      assert.deepEqual(manager.listTopics(), [metadata]);
      const topicDir = join(dataDir, "topics", "orders");
      // One folder per partition plus topic.json; no leftover topic.json.tmp.
      assert.deepEqual((await readdir(topicDir)).sort(), ["0", "1", "2", "topic.json"]);
      const saved: unknown = JSON.parse(await readFile(join(topicDir, "topic.json"), "utf8"));
      assert.deepEqual(saved, { version: 1, name: "orders", partitionCount: 3 });
    });

    test("listTopics returns every topic sorted by name", async () => {
      const manager = await openManager(await createTempDir());
      await manager.createTopic("payments", 1);
      await manager.createTopic("orders", 2);
      const names = manager.listTopics().map((topic) => topic.name);
      assert.deepEqual(names, ["orders", "payments"]);
    });

    test("creating a topic that exists is rejected, also after a restart", async () => {
      const dataDir = await createTempDir();
      const manager = await openManager(dataDir);
      await manager.createTopic("orders", 2);
      await assertRejectsTopicError(manager.createTopic("orders", 2), "TOPIC_EXISTS");
      await manager.close();

      const reopened = await openManager(dataDir);
      await assertRejectsTopicError(reopened.createTopic("orders", 5), "TOPIC_EXISTS");
    });

    test("of two concurrent creates with the same name, exactly one succeeds", async () => {
      const manager = await openManager(await createTempDir());
      // Both start before either finishes.
      const results = await Promise.allSettled([
        manager.createTopic("orders", 2),
        manager.createTopic("orders", 2),
      ]);
      assert.deepEqual(
        results.map((r) => r.status),
        ["fulfilled", "rejected"],
      );
      const failure = results[1];
      assert.ok(failure?.status === "rejected");
      isTopicError("TOPIC_EXISTS")(failure.reason);
    });

    test("invalid names and partition counts are rejected, and nothing is created", async () => {
      const dataDir = await createTempDir();
      const manager = await openManager(dataDir);
      await assertRejectsTopicError(manager.createTopic("Bad/Name", 1), "INVALID_TOPIC_NAME");
      for (const count of [0, -1, 1.5, MAX_PARTITIONS + 1]) {
        await assertRejectsTopicError(manager.createTopic("orders", count), "INVALID_PARTITION_COUNT");
      }
      assert.deepEqual(await readdir(join(dataDir, "topics")), []);
    });
  });

  describe("partitions", () => {
    test("each partition is an independent log with its own offsets", async () => {
      const manager = await openManager(await createTempDir());
      await manager.createTopic("orders", 3);
      await manager.partition("orders", 0).append([record("a"), record("b")]);
      const result = await manager.partition("orders", 2).append([record("c")]);

      assert.deepEqual(result, { baseOffset: 0, lastOffset: 0 }); // partition 2 starts at 0 too
      assert.equal(manager.partition("orders", 0).logEndOffset, 2);
      assert.equal(manager.partition("orders", 1).logEndOffset, 0);
      assert.equal(manager.partition("orders", 2).logEndOffset, 1);
    });

    test("records with the same key land in one partition, in the order sent", async () => {
      const manager = await openManager(await createTempDir());
      await manager.createTopic("orders", 4);
      const partitioner = new Partitioner();

      // 30 records for 3 users, interleaved: user-0, user-1, user-2, user-0, ...
      const sent = new Map<string, string[]>(); // key → values, in sending order
      for (let i = 0; i < 30; i++) {
        const key = `user-${i % 3}`;
        const value = `event-${i}`;
        const partition = partitioner.partition(Buffer.from(key), 4);
        await manager.partition("orders", partition).append([record(value, key)]);
        sent.set(key, [...(sent.get(key) ?? []), value]);
      }

      for (const [key, values] of sent) {
        const partition = partitioner.partition(Buffer.from(key), 4);
        const stored = await manager.partition("orders", partition).read(0, 100);
        const forKey = stored.filter((r) => r.key?.toString() === key);
        assert.deepEqual(valuesOf(forKey), values, key); // all 10, in order
      }
    });

    test("unknown topics and partition numbers are rejected", async () => {
      const manager = await openManager(await createTempDir());
      assertThrowsTopicError(() => manager.partition("missing", 0), "UNKNOWN_TOPIC");
      await manager.createTopic("orders", 2);
      for (const partition of [2, -1, 1.5]) {
        assertThrowsTopicError(() => manager.partition("orders", partition), "UNKNOWN_PARTITION");
      }
    });
  });

  describe("restarts", () => {
    test("topics and their records survive closing and reopening", async () => {
      const dataDir = await createTempDir();
      const first = await openManager(dataDir);
      await first.createTopic("orders", 2);
      await first.createTopic("payments", 1);
      await first.partition("orders", 1).append([record("a"), record("b")]);
      await first.partition("payments", 0).append([record("p")]);
      await first.close();

      const reopened = await openManager(dataDir);
      assert.deepEqual(reopened.listTopics(), [
        { version: 1, name: "orders", partitionCount: 2 },
        { version: 1, name: "payments", partitionCount: 1 },
      ]);
      assert.deepEqual(valuesOf(await reopened.partition("orders", 1).read(0, 10)), ["a", "b"]);
      assert.deepEqual(valuesOf(await reopened.partition("payments", 0).read(0, 10)), ["p"]);
      // Offsets continue where they stopped.
      const next = await reopened.partition("orders", 1).append([record("c")]);
      assert.deepEqual(next, { baseOffset: 2, lastOffset: 2 });
    });

    test("a topic whose creation never finished is ignored, then cleanly re-created", async () => {
      // A crash before the commit point leaves partition data but no topic.json.
      const dataDir = await createTempDir();
      const topicDir = join(dataDir, "topics", "orders");
      await mkdir(join(topicDir, "0"), { recursive: true });
      await writeFile(join(topicDir, "0", "leftover.txt"), "junk");
      await writeFile(join(topicDir, "topic.json.tmp"), "{");

      const manager = await openManager(dataDir);
      assert.deepEqual(manager.listTopics(), []); // not a topic

      await manager.createTopic("orders", 2);
      // The leftovers are gone; the folder holds exactly the new topic.
      assert.deepEqual((await readdir(topicDir)).sort(), ["0", "1", "topic.json"]);
      assert.deepEqual((await readdir(join(topicDir, "0"))).includes("leftover.txt"), false);
    });

    test("folders and files that can't be topics are ignored", async () => {
      const dataDir = await createTempDir();
      await mkdir(join(dataDir, "topics", "Not-A-Topic"), { recursive: true });
      await writeFile(join(dataDir, "topics", "notes.txt"), "hello");
      const manager = await openManager(dataDir);
      assert.deepEqual(manager.listTopics(), []);
    });
  });

  describe("damage", () => {
    test("a damaged topic.json is refused", async () => {
      const damaged = [
        "not json at all",
        "[1, 2, 3]",
        JSON.stringify({ version: 2, name: "orders", partitionCount: 2 }), // unknown version
        JSON.stringify({ version: 1, name: "payments", partitionCount: 2 }), // wrong topic
        JSON.stringify({ version: 1, name: "orders", partitionCount: 0 }), // impossible count
        JSON.stringify({ version: 1, name: "orders", partitionCount: "2" }), // wrong type
      ];
      for (const contents of damaged) {
        const dataDir = await createTempDir();
        const manager = await openManager(dataDir);
        await manager.createTopic("orders", 2);
        await manager.close();
        await writeFile(join(dataDir, "topics", "orders", "topic.json"), contents);

        await assertRejectsTopicError(TopicManager.open(dataDir), "CORRUPT_TOPIC");
      }
    });

    test("a missing partition folder is refused instead of silently re-created empty", async () => {
      const dataDir = await createTempDir();
      const manager = await openManager(dataDir);
      await manager.createTopic("orders", 3);
      await manager.partition("orders", 1).append([record("a")]);
      await manager.close();
      await rm(join(dataDir, "topics", "orders", "1"), { recursive: true });

      await assertRejectsTopicError(TopicManager.open(dataDir), "CORRUPT_TOPIC");
    });

    test("crash recovery is reported for each partition it repaired", async () => {
      const dataDir = await createTempDir();
      const manager = await openManager(dataDir);
      await manager.createTopic("orders", 2);
      await manager.partition("orders", 1).append([record("a")]);
      await manager.close();
      // A crash mid-append on partition 1: only 10 bytes of its next batch landed.
      const torn = encodeBatch({ baseOffset: 1, timestamp: 1_000, records: [record("b")] });
      await appendFile(join(dataDir, "topics", "orders", "1", FIRST_LOG), torn.subarray(0, 10));

      const reopened = await openManager(dataDir);
      assert.equal(reopened.recoveredTails.length, 1);
      const recovery = reopened.recoveredTails[0];
      assert.equal(recovery?.topic, "orders");
      assert.equal(recovery?.partition, 1);
      assert.equal(recovery?.tail.discardedBytes, 10);
      assert.equal(reopened.partition("orders", 1).logEndOffset, 1);
      assert.equal(reopened.partition("orders", 0).logEndOffset, 0); // untouched
    });
  });

  describe("closing", () => {
    test("after close, every method is rejected; closing twice is allowed", async () => {
      const manager = await openManager(await createTempDir());
      await manager.createTopic("orders", 1);
      await manager.close();
      await manager.close();

      await assertRejectsTopicError(manager.createTopic("payments", 1), "MANAGER_CLOSED");
      assertThrowsTopicError(() => manager.listTopics(), "MANAGER_CLOSED");
      assertThrowsTopicError(() => manager.partition("orders", 0), "MANAGER_CLOSED");
    });

    test("close waits for a creation already in progress", async () => {
      const dataDir = await createTempDir();
      const manager = await openManager(dataDir);
      // Start a creation, close immediately, and record the order they finish in.
      const finished: string[] = [];
      const creation = manager.createTopic("orders", 2).then((metadata) => {
        finished.push("created");
        return metadata;
      });
      const closing = manager.close().then(() => {
        finished.push("closed");
      });
      assert.deepEqual(await creation, { version: 1, name: "orders", partitionCount: 2 });
      await closing;
      assert.deepEqual(finished, ["created", "closed"]);

      // The creation completed: the topic is there after a restart.
      const reopened = await openManager(dataDir);
      assert.deepEqual(
        reopened.listTopics().map((topic) => topic.name),
        ["orders"],
      );
    });
  });
});
