// Group consumers against a real broker, with short timeouts so takeovers happen
// in well under a second. Every member has its own connection, as separate
// programs would.

import { after, describe, test } from "node:test";
import assert from "node:assert/strict";

import { Admin } from "../../src/client/admin.js";
import { Connection } from "../../src/client/connection.js";
import { GroupConsumer, type GroupRecord } from "../../src/client/group-consumer.js";
import { Producer } from "../../src/client/producer.js";
import { BrokerServer } from "../../src/server/broker-server.js";
import { TopicManager } from "../../src/topic/topic-manager.js";
import { openGroups } from "../helpers/groups.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

after(removeTempDirs);

const FAST = { heartbeatIntervalMs: 50, joinRetryMs: 20 };

interface Harness {
  /** A new consumer in `group` on its own connection. */
  member(group?: string): Promise<GroupConsumer>;
  /** Produces records without keys, so they're spread one per partition in turn. */
  produce(count: number): Promise<void>;
  /** Drops a member's connection without leaving the group, as a crash would. */
  crash(consumer: GroupConsumer): Promise<void>;
}

// A broker with topic "orders" (3 partitions) and a 400 ms session timeout.
async function withGroupBroker(body: (harness: Harness) => Promise<void>): Promise<void> {
  const dir = await createTempDir();
  const topics = await TopicManager.open(dir, { logOptions: { syncOnAppend: false } });
  const { groups, offsets } = await openGroups(topics, dir, { sessionTimeoutMs: 400, rebalanceTimeoutMs: 400 });
  const server = await BrokerServer.start(topics, groups, { port: 0 });
  const connections = new Map<GroupConsumer, Connection>();
  const admin = await Connection.connect({ port: server.port });
  await new Admin(admin).createTopic("orders", 3);
  const producer = new Producer(admin);
  try {
    await body({
      member: async (group = "billing") => {
        const connection = await Connection.connect({ port: server.port });
        const consumer = new GroupConsumer(connection, group, "orders", FAST);
        connections.set(consumer, connection);
        return consumer;
      },
      produce: async (count) => {
        await producer.send("orders", Array.from({ length: count }, (_, i) => ({ key: null, value: Buffer.from(`r${i}`) })));
      },
      crash: async (consumer) => {
        await connections.get(consumer)?.close();
      },
    });
  } finally {
    for (const [consumer, connection] of connections) {
      await consumer.close().catch(() => {});
      await connection.close().catch(() => {});
    }
    await admin.close();
    await server.close();
    await offsets.close();
    await topics.close();
  }
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

// Runs every consumer in its own poll-and-commit loop, as separate programs would,
// until `done` is true, then stops the loops. Returns what each one read.
// (Polling them in lockstep would not work: during a rebalance a member's poll
// waits for the others to join again, which they can only do in their own loops.)
// With `commit: false` the loops only poll, so the only commits are the ones the
// consumer makes by itself.
async function pollUntil(
  consumers: readonly GroupConsumer[],
  done: (reads: GroupRecord[][]) => boolean,
  { timeoutMs = 5000, commit = true }: { timeoutMs?: number; commit?: boolean } = {},
): Promise<GroupRecord[][]> {
  const reads: GroupRecord[][] = consumers.map(() => []);
  let stop = false;
  let loopError: unknown = null;
  const loops = consumers.map(async (consumer, i) => {
    try {
      while (!stop) {
        reads[i]!.push(...(await consumer.poll()));
        if (commit) {
          await consumer.commit();
        }
        await sleep(10);
      }
    } catch (error) {
      loopError ??= error;
    }
  });
  const deadline = Date.now() + timeoutMs;
  try {
    while (!done(reads)) {
      if (loopError !== null) {
        throw loopError;
      }
      if (Date.now() > deadline) {
        assert.fail(`timed out; read so far: ${JSON.stringify(reads.map((r) => r.map(label)))}`);
      }
      await sleep(10);
    }
  } finally {
    stop = true;
    await Promise.all(loops);
  }
  return reads;
}

const label = (record: GroupRecord): string => `${record.partition}@${record.offset}`;

// True once the members agree on a generation and own every partition between them, once.
function stable(consumers: readonly GroupConsumer[]): boolean {
  const generations = new Set(consumers.map((c) => c.generation));
  const owned = consumers.flatMap((c) => c.partitions).sort();
  return generations.size === 1 && !generations.has(0) && owned.join(",") === "0,1,2";
}

describe("GroupConsumer", () => {
  test("members split the partitions, and each record is read once, by its partition's owner", async () => {
    await withGroupBroker(async ({ member, produce }) => {
      const a = await member();
      const b = await member();
      await pollUntil([a, b], () => stable([a, b]));
      assert.deepEqual(new Set([a.partitions.length, b.partitions.length]), new Set([1, 2]));

      await produce(9); // 3 per partition
      const reads = await pollUntil([a, b], (r) => r.flat().length >= 9);
      const all = reads.flat().map(label);
      assert.equal(new Set(all).size, 9, "every record exactly once");
      reads.forEach((records, i) => {
        const owner = [a, b][i]!;
        assert.ok(records.every((r) => owner.partitions.includes(r.partition)), "only from partitions it owns");
      });
    });
  });

  test("a member that leaves hands over at once, and the next owner continues from its commits", async () => {
    await withGroupBroker(async ({ member, produce }) => {
      const a = await member();
      const b = await member();
      await pollUntil([a, b], () => stable([a, b]));
      await produce(6); // offsets 0 and 1 in every partition
      await pollUntil([a, b], (r) => r.flat().length >= 6);
      const bPartitions = b.partitions;
      await b.close(); // commits, then leaves

      await produce(3); // offset 2 in every partition
      const [afterLeave] = await pollUntil([a], (r) => a.partitions.length === 3 && r[0]!.length >= 3);
      // A took over B's partitions from B's committed offsets: it never re-reads offsets 0 and 1 there.
      const fromB = afterLeave!.filter((r) => bPartitions.includes(r.partition));
      assert.ok(fromB.length > 0);
      assert.ok(fromB.every((r) => r.offset === 2), `re-read B's records: ${fromB.map(label)}`);
    });
  });

  test("a member commits what it has read before joining a rebalance, so the next owner doesn't read it again", async () => {
    await withGroupBroker(async ({ member, produce }) => {
      const a = await member();
      await produce(3); // one record per partition
      await pollUntil([a], (r) => r[0]!.length >= 3, { commit: false });

      // A never called commit(). When B joins, A must commit its progress as part of
      // rejoining, or B starts its partitions at offset 0 and reads those records again.
      const b = await member();
      const [, whileJoining] = await pollUntil([a, b], () => stable([a, b]), { commit: false });
      const [, afterwards] = await pollUntil([a, b], () => true, { commit: false }); // one more round
      assert.deepEqual([...whileJoining!, ...afterwards!].map(label), []);
    });
  });

  test("a member that crashes is replaced after the session timeout", async () => {
    await withGroupBroker(async ({ member, produce, crash }) => {
      const a = await member();
      const b = await member();
      await pollUntil([a, b], () => stable([a, b]));
      const before = a.generation;
      await crash(b); // no leave: the broker only notices when the heartbeats stop
      await produce(3);
      await pollUntil([a], () => a.partitions.length === 3);
      assert.ok(a.generation > before);
    });
  });

  test("committed offsets carry over to the group's next member, but not to another group", async () => {
    await withGroupBroker(async ({ member, produce }) => {
      await produce(3);
      const first = await member();
      await pollUntil([first], (r) => r[0]!.length >= 3);
      await first.close();

      // The same group continues after the 3 records it already processed.
      const next = await member();
      const nextReads = await pollUntil([next], () => next.partitions.length === 3);
      await pollUntil([next], () => true); // one more round
      assert.deepEqual(nextReads[0], []);

      // Another group has its own offsets, so it starts from the beginning.
      const other = await member("email");
      const otherReads = await pollUntil([other], (r) => r[0]!.length >= 3);
      assert.equal(otherReads[0]!.length, 3);
    });
  });

  test("more members than partitions: the extra member gets none and waits", async () => {
    await withGroupBroker(async ({ member }) => {
      const consumers = [await member(), await member(), await member(), await member()];
      await pollUntil(consumers, () => {
        const owned = consumers.flatMap((c) => c.partitions).sort().join(",");
        return new Set(consumers.map((c) => c.generation)).size === 1 && owned === "0,1,2";
      });
      assert.equal(consumers.filter((c) => c.partitions.length === 0).length, 1);
    });
  });
});
