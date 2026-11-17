// ============================================================================
// Tests for offset-store.ts.
//
// Committed offsets must: come back exactly after a restart (replay), follow
// "the latest commit wins", keep every (group, topic, partition) separate,
// survive a crash in the middle of a commit, and refuse to start from a log
// they can't fully read.
// ============================================================================

import { after, describe, test } from "node:test";
import assert from "node:assert/strict";
import { appendFile } from "node:fs/promises";
import { join } from "node:path";

import { OffsetStore } from "../../src/group/offset-store.js";
import { FileLog } from "../../src/storage/file-log.js";
import { LogError } from "../../src/storage/log.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";

after(removeTempDirs);

// The first (and here, only) segment file of a log stored in `dir`.
const segmentFile = (dir: string): string => join(dir, "00000000000000000000.log");

// Opens a store in `dir`, runs `body` with it, and always closes it (Windows
// can't delete open files). syncOnAppend: false skips fsync, which only makes
// these tests slower; the crash test below keeps fsync on.
async function withStore(dir: string, body: (store: OffsetStore) => Promise<void>): Promise<void> {
  const store = await OffsetStore.open(dir, { syncOnAppend: false });
  try {
    await body(store);
  } finally {
    await store.close();
  }
}

// Accepts only a LogError with code CORRUPT_LOG (used with assert.rejects).
function isCorruptLog(err: unknown): boolean {
  assert.ok(err instanceof LogError, "expected a LogError");
  assert.equal(err.code, "CORRUPT_LOG");
  return true;
}

describe("OffsetStore", () => {
  test("a position nobody has committed is null", async () => {
    await withStore(await createTempDir(), async (store) => {
      assert.equal(store.get("billing", "orders", 0), null);
      assert.equal(store.size, 0);
    });
  });

  test("the latest commit wins, and an offset may move backwards", async () => {
    await withStore(await createTempDir(), async (store) => {
      await store.commit("billing", "orders", 2, 50);
      await store.commit("billing", "orders", 2, 57);
      assert.equal(store.get("billing", "orders", 2), 57);
      // Going back to re-process records is allowed.
      await store.commit("billing", "orders", 2, 10);
      assert.equal(store.get("billing", "orders", 2), 10);
      assert.equal(store.size, 1); // still one position
    });
  });

  test("group, topic and partition each keep their own offset", async () => {
    await withStore(await createTempDir(), async (store) => {
      await store.commit("billing", "orders", 0, 5);
      await store.commit("email", "orders", 0, 9); // another group
      await store.commit("billing", "payments", 0, 3); // another topic
      await store.commit("billing", "orders", 1, 7); // another partition
      // Names that would collide if the parts were simply joined with "|".
      await store.commit("a|b", "c", 0, 1);
      await store.commit("a", "b|c", 0, 2);

      assert.equal(store.get("billing", "orders", 0), 5);
      assert.equal(store.get("email", "orders", 0), 9);
      assert.equal(store.get("billing", "payments", 0), 3);
      assert.equal(store.get("billing", "orders", 1), 7);
      assert.equal(store.get("a|b", "c", 0), 1);
      assert.equal(store.get("a", "b|c", 0), 2);
      assert.equal(store.size, 6);
    });
  });

  test("committed offsets come back after closing and reopening", async () => {
    const dir = await createTempDir();
    await withStore(dir, async (store) => {
      await store.commit("billing", "orders", 0, 5);
      await store.commit("billing", "orders", 0, 8); // the one that must come back
      await store.commit("email", "orders", 1, 3);
    });
    // A brand-new store, as after a broker restart: it only has the log.
    await withStore(dir, async (store) => {
      assert.equal(store.get("billing", "orders", 0), 8);
      assert.equal(store.get("email", "orders", 1), 3);
      assert.equal(store.size, 2);
    });
  });

  test("replay reads a long log in chunks without missing a record", async () => {
    // 1,205 commits is more than one replay chunk (1,000 records), so replay
    // has to read several chunks and carry on exactly where each one ended.
    // Every commit uses its own partition number, so skipping any single
    // record would lose a key. Partition 0 is committed again at the very end,
    // so "the latest commit wins" is checked across a chunk boundary too.
    const dir = await createTempDir();
    await withStore(dir, async (store) => {
      for (let i = 0; i < 1205; i++) {
        await store.commit("billing", "orders", i, i + 1);
      }
      await store.commit("billing", "orders", 0, 9999);
    });
    await withStore(dir, async (store) => {
      assert.equal(store.size, 1205);
      assert.equal(store.get("billing", "orders", 0), 9999);
      for (let i = 1; i < 1205; i++) {
        assert.equal(store.get("billing", "orders", i), i + 1, `partition ${i}`);
      }
    });
  });

  test("commits made at the same time are applied in call order", async () => {
    const dir = await createTempDir();
    await withStore(dir, async (store) => {
      // Started together, without waiting for each other: the log's append
      // queue keeps them in order, so the last call wins.
      await Promise.all([
        store.commit("billing", "orders", 0, 1),
        store.commit("billing", "orders", 0, 2),
        store.commit("billing", "orders", 0, 3),
      ]);
      assert.equal(store.get("billing", "orders", 0), 3);
    });
    await withStore(dir, async (store) => {
      assert.equal(store.get("billing", "orders", 0), 3); // the same after replay
    });
  });

  test("a commit torn by a crash is cut off on reopen; earlier commits survive", async () => {
    const dir = await createTempDir();
    // fsync stays on here, as in a real broker.
    const store = await OffsetStore.open(dir);
    await store.commit("billing", "orders", 0, 5);
    await store.commit("billing", "orders", 1, 9);
    await store.close();
    // What a crash in the middle of a third commit leaves behind: the start of
    // a batch whose length says 99 bytes, followed by only 3 of them.
    await appendFile(segmentFile(dir), Buffer.from([0, 0, 0, 99, 1, 2, 3]));

    const reopened = await OffsetStore.open(dir);
    try {
      assert.notEqual(reopened.recoveredTail, null, "recovery should report the torn commit");
      assert.equal(reopened.recoveredTail?.discardedBytes, 7);
      assert.equal(reopened.get("billing", "orders", 0), 5);
      assert.equal(reopened.get("billing", "orders", 1), 9);
    } finally {
      await reopened.close();
    }
  });

  test("a record that isn't a commit stops the store from opening", async () => {
    // Each case writes one good commit, then one bad record straight into the
    // log (bypassing the store), and expects OffsetStore.open to refuse.
    const goodKey = Buffer.from([0, 7, ...Buffer.from("billing"), 0, 6, ...Buffer.from("orders"), 0, 0, 0, 0]);
    const goodValue = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 5]); // version 0, offset 5
    const badRecords = [
      { name: "no key", key: null, value: goodValue },
      { name: "key cut short", key: goodKey.subarray(0, 5), value: goodValue },
      { name: "extra byte after the key", key: Buffer.concat([goodKey, Buffer.from([1])]), value: goodValue },
      { name: "unknown format version", key: goodKey, value: Buffer.from([9, 0, 0, 0, 0, 0, 0, 0, 5]) },
      { name: "value cut short", key: goodKey, value: goodValue.subarray(0, 4) },
    ];
    for (const bad of badRecords) {
      const dir = await createTempDir();
      const log = await FileLog.open(dir, { syncOnAppend: false });
      await log.append([{ key: goodKey, value: goodValue }]);
      await log.append([{ key: bad.key, value: bad.value }]);
      await log.close();
      await assert.rejects(OffsetStore.open(dir), isCorruptLog, bad.name);
    }
  });

  test("the hand-built good record above really is a valid commit", async () => {
    // Proves the corrupt-record test fails because of the bad record, not
    // because its "good" record was wrong too.
    const dir = await createTempDir();
    const log = await FileLog.open(dir, { syncOnAppend: false });
    const key = Buffer.from([0, 7, ...Buffer.from("billing"), 0, 6, ...Buffer.from("orders"), 0, 0, 0, 0]);
    await log.append([{ key, value: Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 5]) }]);
    await log.close();
    await withStore(dir, async (store) => {
      assert.equal(store.get("billing", "orders", 0), 5);
    });
  });

  test("invalid arguments are refused before anything is written", async () => {
    const dir = await createTempDir();
    await withStore(dir, async (store) => {
      // Each entry is a call that must throw RangeError.
      const badCommits = [
        () => store.commit("", "orders", 0, 1),
        () => store.commit("billing", "", 0, 1),
        () => store.commit("billing", "orders", -1, 1),
        () => store.commit("billing", "orders", 0.5, 1),
        () => store.commit("billing", "orders", 0, -1),
        () => store.commit("billing", "orders", 0, 1.5),
      ];
      for (const [index, commit] of badCommits.entries()) {
        await assert.rejects(commit(), RangeError, `bad commit #${index}`);
      }
      assert.equal(store.size, 0);
    });
    // Nothing reached the log either.
    await withStore(dir, async (store) => {
      assert.equal(store.size, 0);
    });
  });

  test("after close, the store refuses work; closing twice is fine", async () => {
    const store = await OffsetStore.open(await createTempDir(), { syncOnAppend: false });
    await store.commit("billing", "orders", 0, 1);
    await store.close();
    const isClosed = (err: unknown): boolean => err instanceof LogError && err.code === "LOG_CLOSED";
    await assert.rejects(store.commit("billing", "orders", 0, 2), isClosed);
    assert.throws(() => store.get("billing", "orders", 0), isClosed);
    await store.close();
  });
});
