// The coordinator is driven with a fake clock: `now` only moves when a test moves
// it, and timeouts apply only when the test calls tick().

import { after, describe, test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";

import { GroupCoordinator, assignRoundRobin } from "../../src/group/group-coordinator.js";
import { GroupError, type GroupErrorCode } from "../../src/group/group-error.js";
import { OffsetStore } from "../../src/group/offset-store.js";
import { LogError } from "../../src/storage/log.js";
import { TopicManager } from "../../src/topic/topic-manager.js";
import { TopicError } from "../../src/topic/topic-metadata.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

after(removeTempDirs);

const SESSION_MS = 1000;
const REBALANCE_MS = 500;

interface Setup {
  readonly groups: GroupCoordinator;
  readonly topics: TopicManager;
  readonly offsets: OffsetStore;
  readonly clock: { now: number };
}

// A topic "orders" with 3 partitions (one record in partition 0), an offset
// store, and a coordinator on a fake clock. Always closed afterwards.
async function withGroups(body: (setup: Setup) => Promise<void>, dataDir?: string): Promise<void> {
  const dir = dataDir ?? (await createTempDir());
  const topics = await TopicManager.open(dir, { logOptions: { syncOnAppend: false } });
  const offsets = await OffsetStore.open(join(dir, "offsets"), { syncOnAppend: false });
  try {
    if (topics.listTopics().length === 0) {
      await topics.createTopic("orders", 3);
      await topics.createTopic("payments", 1);
      await topics.partition("orders", 0).append([{ key: null, value: Buffer.from("x") }]);
    }
    const clock = { now: 0 };
    const groups = new GroupCoordinator(topics, offsets, {
      sessionTimeoutMs: SESSION_MS,
      rebalanceTimeoutMs: REBALANCE_MS,
      clock: () => clock.now,
    });
    await body({ groups, topics, offsets, clock });
  } finally {
    await offsets.close();
    await topics.close();
  }
}

function isGroupError(code: GroupErrorCode): (err: unknown) => boolean {
  return (err: unknown) => {
    assert.ok(err instanceof GroupError, `expected GroupError ${code}, got ${String(err)}`);
    assert.equal(err.code, code);
    return true;
  };
}

// Two members, A and B, joined and stable in generation 2.
function twoMembers(groups: GroupCoordinator): { a: string; b: string } {
  const a = groups.join("billing", "orders", "").memberId; // generation 1, alone
  const b = groups.join("billing", "orders", "").memberId; // starts a rebalance
  groups.join("billing", "orders", a); // A joins again: everyone is ready
  return { a, b };
}

describe("GroupCoordinator: joining and rebalancing", () => {
  test("the first member is assigned every partition at once", async () => {
    await withGroups(async ({ groups }) => {
      assert.deepEqual(groups.join("billing", "orders", ""), {
        memberId: "member-1",
        status: "assigned",
        generation: 1,
        partitions: [0, 1, 2],
      });
    });
  });

  test("a new member makes everyone join again before anyone gets the new assignment", async () => {
    await withGroups(async ({ groups }) => {
      const a = groups.join("billing", "orders", "").memberId;
      const b = groups.join("billing", "orders", "");
      // B waits: A still reads all three partitions and hasn't joined again.
      assert.equal(b.status, "waiting");
      assert.throws(() => groups.heartbeat("billing", a, 1), isGroupError("REBALANCE_IN_PROGRESS"));
      // A joins again, which completes the rebalance: generation 2, partitions dealt in turn.
      assert.deepEqual(groups.join("billing", "orders", a), { memberId: a, status: "assigned", generation: 2, partitions: [0, 2] });
      assert.deepEqual(groups.join("billing", "orders", b.memberId), { memberId: b.memberId, status: "assigned", generation: 2, partitions: [1] });
      groups.heartbeat("billing", a, 2); // stable again
      groups.heartbeat("billing", b.memberId, 2);
    });
  });

  test("partitions are dealt out in turn; extra members get none", () => {
    assert.deepEqual([...assignRoundRobin(["a", "b"], 3)], [["a", [0, 2]], ["b", [1]]]);
    assert.deepEqual([...assignRoundRobin(["a", "b", "c", "d"], 3)], [["a", [0]], ["b", [1]], ["c", [2]], ["d", []]]);
    assert.deepEqual([...assignRoundRobin(["a"], 0)], [["a", []]]);
  });

  test("a member that leaves hands its partitions over without waiting for a timeout", async () => {
    await withGroups(async ({ groups }) => {
      const { a, b } = twoMembers(groups);
      groups.leave("billing", b);
      assert.throws(() => groups.heartbeat("billing", a, 2), isGroupError("REBALANCE_IN_PROGRESS"));
      assert.deepEqual(groups.join("billing", "orders", a).partitions, [0, 1, 2]);
      assert.throws(() => groups.heartbeat("billing", b, 2), isGroupError("UNKNOWN_MEMBER"));
    });
  });

  test("a silent member is removed after the session timeout, and the others take over", async () => {
    await withGroups(async ({ groups, clock }) => {
      const { a, b } = twoMembers(groups);
      // A keeps sending heartbeats; B goes silent.
      for (let t = 400; t <= 1200; t += 400) {
        clock.now = t;
        if (t < 1200) {
          groups.heartbeat("billing", a, 2);
        }
        groups.tick();
      }
      // At 1200 ms B has been silent for over 1000 ms: removed, so A must join again.
      assert.throws(() => groups.heartbeat("billing", a, 2), isGroupError("REBALANCE_IN_PROGRESS"));
      assert.deepEqual(groups.join("billing", "orders", a), { memberId: a, status: "assigned", generation: 3, partitions: [0, 1, 2] });
      assert.throws(() => groups.heartbeat("billing", b, 2), isGroupError("UNKNOWN_MEMBER"));
    });
  });

  test("a member that doesn't join again within the rebalance timeout is removed", async () => {
    await withGroups(async ({ groups, clock }) => {
      const { a, b } = twoMembers(groups);
      const c = groups.join("billing", "orders", "").memberId; // rebalance starts at time 0
      groups.join("billing", "orders", a); // A is ready
      // B stays alive (it keeps sending heartbeats) but never joins again.
      clock.now = 400;
      assert.throws(() => groups.heartbeat("billing", b, 2), isGroupError("REBALANCE_IN_PROGRESS"));
      groups.tick();
      assert.equal(groups.join("billing", "orders", c).status, "waiting");
      clock.now = REBALANCE_MS; // the deadline
      groups.tick();
      // B is removed; A and C share the partitions in generation 3.
      assert.deepEqual(groups.join("billing", "orders", a), { memberId: a, status: "assigned", generation: 3, partitions: [0, 2] });
      assert.deepEqual(groups.join("billing", "orders", c).partitions, [1]);
      assert.throws(() => groups.heartbeat("billing", b, 2), isGroupError("UNKNOWN_MEMBER"));
    });
  });

  test("a member waiting for a rebalance stays alive by asking again", async () => {
    await withGroups(async ({ groups, clock }) => {
      const { a } = twoMembers(groups);
      const c = groups.join("billing", "orders", "").memberId;
      // C keeps asking; A and B never join again, so after the rebalance timeout
      // they are removed and C, which stayed alive, gets everything.
      for (clock.now = 100; clock.now <= 600; clock.now += 100) {
        groups.join("billing", "orders", c);
        groups.tick();
      }
      assert.deepEqual(groups.join("billing", "orders", c).partitions, [0, 1, 2]);
      assert.throws(() => groups.heartbeat("billing", a, 2), isGroupError("UNKNOWN_MEMBER"));
    });
  });

  test("an empty group may switch topics; a group with members may not", async () => {
    await withGroups(async ({ groups }) => {
      const a = groups.join("billing", "orders", "").memberId;
      assert.throws(() => groups.join("billing", "payments", ""), isGroupError("INCONSISTENT_GROUP_TOPIC"));
      groups.leave("billing", a);
      // The generation keeps counting, so old members' requests stay stale.
      assert.deepEqual(groups.join("billing", "payments", ""), { memberId: "member-2", status: "assigned", generation: 2, partitions: [0] });
    });
  });

  test("invalid group ids, unknown members and unknown topics are refused", async () => {
    await withGroups(async ({ groups }) => {
      for (const bad of ["", "Billing", "-billing", "a b", "x".repeat(101)]) {
        assert.throws(() => groups.join(bad, "orders", ""), isGroupError("INVALID_GROUP_ID"), bad);
      }
      assert.throws(() => groups.join("billing", "orders", "member-99"), isGroupError("UNKNOWN_MEMBER"));
      assert.throws(() => groups.heartbeat("nobody", "member-1", 1), isGroupError("UNKNOWN_MEMBER"));
      assert.throws(() => groups.join("billing", "missing", ""), (err: unknown) => err instanceof TopicError && err.code === "UNKNOWN_TOPIC");
    });
  });
});

describe("GroupCoordinator: committed offsets and fencing", () => {
  test("the partition's owner commits; anyone can fetch the committed offset", async () => {
    await withGroups(async ({ groups }) => {
      const a = groups.join("billing", "orders", "").memberId;
      assert.equal(groups.fetchOffset("billing", "orders", 0), null);
      await groups.commit("billing", a, 1, "orders", 0, 1);
      assert.equal(groups.fetchOffset("billing", "orders", 0), 1);
      assert.equal(groups.fetchOffset("email", "orders", 0), null); // another group has its own offsets
    });
  });

  test("a commit with an older generation is refused, so stale progress can't overwrite newer", async () => {
    await withGroups(async ({ groups }) => {
      const { a } = twoMembers(groups); // A owns partitions 0 and 2 in generation 2
      await assert.rejects(groups.commit("billing", a, 1, "orders", 0, 1), isGroupError("STALE_GENERATION"));
      assert.throws(() => groups.heartbeat("billing", a, 1), isGroupError("STALE_GENERATION"));
      assert.equal(groups.fetchOffset("billing", "orders", 0), null);
    });
  });

  test("a removed member's commits are refused", async () => {
    await withGroups(async ({ groups, clock }) => {
      const a = groups.join("billing", "orders", "").memberId;
      // A pauses for longer than the session timeout, and is removed.
      clock.now = SESSION_MS + 1;
      groups.tick();
      const b = groups.join("billing", "orders", "").memberId; // B now owns every partition
      await groups.commit("billing", b, 2, "orders", 0, 1);
      // A wakes up and tries to save its old progress.
      await assert.rejects(groups.commit("billing", a, 1, "orders", 0, 0), isGroupError("UNKNOWN_MEMBER"));
      assert.equal(groups.fetchOffset("billing", "orders", 0), 1);
    });
  });

  test("only the owner may commit a partition, and only within its log", async () => {
    await withGroups(async ({ groups }) => {
      const { a, b } = twoMembers(groups); // A: 0 and 2, B: 1
      await assert.rejects(groups.commit("billing", b, 2, "orders", 0, 1), isGroupError("NOT_PARTITION_OWNER"));
      await assert.rejects(groups.commit("billing", a, 2, "payments", 0, 0), isGroupError("NOT_PARTITION_OWNER"));
      // Partition 0 holds one record, so its log ends at 1.
      await assert.rejects(
        groups.commit("billing", a, 2, "orders", 0, 2),
        (err: unknown) => err instanceof LogError && err.code === "OFFSET_OUT_OF_RANGE",
      );
      await groups.commit("billing", a, 2, "orders", 0, 1);
    });
  });

  test("during a rebalance, members can still commit their current partitions", async () => {
    await withGroups(async ({ groups }) => {
      const a = groups.join("billing", "orders", "").memberId;
      groups.join("billing", "orders", ""); // a rebalance starts
      await groups.commit("billing", a, 1, "orders", 0, 1); // A saves its progress first
      assert.equal(groups.fetchOffset("billing", "orders", 0), 1);
    });
  });

  test("committed offsets survive a broker restart; membership doesn't", async () => {
    const dir = await createTempDir();
    let member = "";
    await withGroups(async ({ groups }) => {
      member = groups.join("billing", "orders", "").memberId;
      await groups.commit("billing", member, 1, "orders", 0, 1);
    }, dir);
    await withGroups(async ({ groups }) => {
      assert.equal(groups.fetchOffset("billing", "orders", 0), 1);
      assert.throws(() => groups.heartbeat("billing", member, 1), isGroupError("UNKNOWN_MEMBER"));
    }, dir);
  });
});
