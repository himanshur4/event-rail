// Part 1 runs the shared contract tests twice: with the default segment size,
// and with tiny segments so every rule is also checked across many segments.
// Part 2 covers persistence across restarts, the exact on-disk bytes, and
// refusing damaged files. Part 3 covers rolling, index repair, and missing
// segment files.

import { after, afterEach, describe, test } from "node:test";
import assert from "node:assert/strict";
import { appendFile, readFile, readdir, rm, truncate, writeFile } from "node:fs/promises";
import { join } from "node:path";

import type { RecordInput } from "../../src/common/types.js";
import { FileLog, type FileLogOptions } from "../../src/storage/file-log.js";
import { LogError } from "../../src/storage/log.js";
import { encodeBatch } from "../../src/storage/record-batch.js";
import { createTempDir, removeTempDirs } from "../helpers/temp-dir.js";
import { runLogContractTests } from "./log-contract.js";

// The first segment's file names (base offset 0, padded to 20 digits).
const FIRST_LOG = "00000000000000000000.log";
const FIRST_INDEX = "00000000000000000000.index";

after(removeTempDirs);

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

const fixedClock = () => 1_000;

// Every log a test opens is closed after it, even if the test failed: on
// Windows a directory can't be deleted while a file in it is open.
const openLogs: FileLog[] = [];
async function openLog(dir: string, options: FileLogOptions = {}): Promise<FileLog> {
  const log = await FileLog.open(dir, { clock: fixedClock, ...options });
  openLogs.push(log);
  return log;
}
afterEach(async () => {
  for (const log of openLogs.splice(0)) {
    await log.close();
  }
});

function record(value: string, key: string | null = null): RecordInput {
  return { key: key === null ? null : Buffer.from(key), value: Buffer.from(value) };
}

async function assertCorrupt(promise: Promise<unknown>): Promise<void> {
  await assert.rejects(promise, (err: unknown) => {
    assert.ok(err instanceof LogError);
    assert.equal(err.code, "CORRUPT_LOG");
    return true;
  });
}

async function filesIn(dir: string): Promise<string[]> {
  return (await readdir(dir)).sort();
}

// ---------------------------------------------------------------------------
// Part 1: the shared Log contract
// ---------------------------------------------------------------------------

runLogContractTests("FileLog", async (clock) => FileLog.open(await createTempDir(), { clock }));

// 64 bytes holds only one small batch, so almost every append starts a new segment.
runLogContractTests("FileLog with tiny segments", async (clock) =>
  FileLog.open(await createTempDir(), { clock, segmentMaxBytes: 64 }),
);

// ---------------------------------------------------------------------------
// Part 2: persistence
// ---------------------------------------------------------------------------

describe("FileLog persistence", () => {
  test("records survive closing and reopening the log", async () => {
    const dir = await createTempDir();
    const first = await openLog(dir);
    await first.append([record("a", "k1"), record("b")]);
    await first.append([record("c")]);
    await first.close();

    const reopened = await openLog(dir);
    assert.equal(reopened.logEndOffset, 3);
    assert.deepEqual(await reopened.read(0, 10), [
      { offset: 0, timestamp: 1_000, key: Buffer.from("k1"), value: Buffer.from("a") },
      { offset: 1, timestamp: 1_000, key: null, value: Buffer.from("b") },
      { offset: 2, timestamp: 1_000, key: null, value: Buffer.from("c") },
    ]);
  });

  test("offsets continue from where they stopped after reopening", async () => {
    const dir = await createTempDir();
    const first = await openLog(dir);
    await first.append([record("a"), record("b")]);
    await first.close();

    const reopened = await openLog(dir);
    const result = await reopened.append([record("c")]);
    assert.deepEqual(result, { baseOffset: 2, lastOffset: 2 }); // not back to 0
  });

  test("the file holds exactly the encoded batches, back to back", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir);
    await log.append([record("a")]);
    await log.append([record("b"), record("c")]);
    await log.close();

    // What the bytes SHOULD be, built directly with encodeBatch.
    const expected = Buffer.concat([
      encodeBatch({ baseOffset: 0, timestamp: 1_000, records: [record("a")] }),
      encodeBatch({ baseOffset: 1, timestamp: 1_000, records: [record("b"), record("c")] }),
    ]);
    assert.deepEqual(await readFile(join(dir, FIRST_LOG)), expected);
  });

  test("concurrent appends are all persisted in call order", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir);
    // 20 appends started at once; the queue must write them one by one.
    await Promise.all(Array.from({ length: 20 }, (_, i) => log.append([record(`${i}`)])));
    await log.close();

    const reopened = await openLog(dir);
    const values = (await reopened.read(0, 100)).map((r) => r.value.toString());
    assert.deepEqual(
      values,
      Array.from({ length: 20 }, (_, i) => `${i}`),
    );
  });

  test("data is persisted with syncOnAppend disabled", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir, { syncOnAppend: false });
    await log.append([record("a")]);
    await log.close();

    const reopened = await openLog(dir);
    assert.equal(reopened.logEndOffset, 1);
  });

  test("opening creates the directory if it does not exist", async () => {
    const dir = join(await createTempDir(), "nested", "partition-0");
    const log = await openLog(dir);
    assert.equal(log.logEndOffset, 0);
  });

  // Until crash recovery exists (Milestone 3), a damaged file must be refused,
  // never partly loaded.
  test("opening a file that ends in a partially written batch is refused", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir);
    await log.append([record("a")]);
    await log.close();

    // Simulate a crash mid-append: add only the first 10 bytes of another batch.
    const torn = encodeBatch({ baseOffset: 1, timestamp: 1_000, records: [record("b")] });
    await appendFile(join(dir, FIRST_LOG), torn.subarray(0, 10));

    await assertCorrupt(FileLog.open(dir));
  });

  test("opening a file with a damaged byte is refused", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir);
    await log.append([record("hello")]);
    await log.close();

    const path = join(dir, FIRST_LOG);
    const bytes = await readFile(path);
    bytes[bytes.length - 1] = (bytes[bytes.length - 1] ?? 0) ^ 0xff; // flip the last byte
    await writeFile(path, bytes);

    await assertCorrupt(FileLog.open(dir));
  });

  test("opening a file with a gap in offsets is refused", async () => {
    const dir = await createTempDir();
    // A valid batch, but starting at offset 5 instead of 0.
    const wrongStart = encodeBatch({ baseOffset: 5, timestamp: 1_000, records: [record("a")] });
    await writeFile(join(dir, FIRST_LOG), wrongStart);

    await assertCorrupt(FileLog.open(dir));
  });
});

// ---------------------------------------------------------------------------
// Part 3: segments and index files
// ---------------------------------------------------------------------------
// A batch holding one record with no key and a 1-byte value is exactly
// 37 bytes: 28 (header) + 4 (keyLength) + 4 (valueLength) + 1 (value).
// With segmentMaxBytes = 100, a segment fits two of them (74 bytes); a third
// (74 + 37 = 111 > 100) starts a new segment.

describe("FileLog segments", () => {
  // Appends five 37-byte batches with a 100-byte limit, producing segments
  // starting at offsets 0 (offsets 0-1), 2 (offsets 2-3), and 4 (offset 4).
  async function writeFiveBatches(dir: string): Promise<FileLog> {
    const log = await openLog(dir, { segmentMaxBytes: 100 });
    for (const value of ["a", "b", "c", "d", "e"]) {
      await log.append([record(value)]);
    }
    return log;
  }

  test("a new segment starts when the active one would exceed segmentMaxBytes", async () => {
    const dir = await createTempDir();
    const log = await writeFiveBatches(dir);
    assert.equal(log.logEndOffset, 5);
    assert.deepEqual(await filesIn(dir), [
      "00000000000000000000.index",
      "00000000000000000000.log",
      "00000000000000000002.index",
      "00000000000000000002.log",
      "00000000000000000004.index",
      "00000000000000000004.log",
    ]);
  });

  test("records and offsets survive reopening across segments", async () => {
    const dir = await createTempDir();
    await (await writeFiveBatches(dir)).close();

    const reopened = await openLog(dir, { segmentMaxBytes: 100 });
    const values = (await reopened.read(0, 10)).map((r) => r.value.toString());
    assert.deepEqual(values, ["a", "b", "c", "d", "e"]);
    // The next append continues at offset 5 in the active segment.
    assert.deepEqual(await reopened.append([record("f")]), { baseOffset: 5, lastOffset: 5 });
  });

  test("a batch larger than segmentMaxBytes is stored alone in its own segment", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir, { segmentMaxBytes: 50 });
    await log.append([record("a")]); // 37 bytes -> segment 0
    await log.append([record("x".repeat(200))]); // too big to share -> segment 1, alone
    await log.append([record("b")]); // segment 1 is over the limit -> segment 2
    const logFiles = (await filesIn(dir)).filter((name) => name.endsWith(".log"));
    assert.deepEqual(logFiles, [
      "00000000000000000000.log",
      "00000000000000000001.log",
      "00000000000000000002.log",
    ]);
    const values = (await log.read(0, 10)).map((r) => r.value.length);
    assert.deepEqual(values, [1, 200, 1]);
  });

  test("the index file holds one 8-byte entry per batch", async () => {
    const dir = await createTempDir();
    const log = await openLog(dir);
    await log.append([record("a")]); // 37 bytes at position 0
    await log.append([record("b"), record("c")]); // starts at offset 1, position 37
    await log.close();

    // Build the expected entries by hand: [relative offset, position] pairs.
    const expected = Buffer.alloc(16);
    expected.writeUInt32BE(0, 0); // batch 1: relative offset 0 ...
    expected.writeUInt32BE(0, 4); //          ... at byte 0
    expected.writeUInt32BE(1, 8); // batch 2: relative offset 1 ...
    expected.writeUInt32BE(37, 12); //        ... at byte 37
    assert.deepEqual(await readFile(join(dir, FIRST_INDEX)), expected);
  });

  // The next five tests damage ONLY the index. The .log is the source of truth,
  // so each time the log must open normally and the index must be repaired to
  // exactly what it was.
  describe("a bad index is repaired from the .log", () => {
    // Writes three batches, closes the log, and returns the folder and the
    // index file's correct contents so a test can damage it and compare later.
    async function prepare(): Promise<{ dir: string; goodIndex: Buffer }> {
      const dir = await createTempDir();
      const log = await openLog(dir);
      await log.append([record("a")]);
      await log.append([record("b")]);
      await log.append([record("c")]);
      await log.close();
      return { dir, goodIndex: await readFile(join(dir, FIRST_INDEX)) };
    }

    // Reopens the log and checks that reads work and the index was restored.
    async function assertRepaired(dir: string, goodIndex: Buffer): Promise<void> {
      const log = await openLog(dir);
      const values = (await log.read(0, 10)).map((r) => r.value.toString());
      assert.deepEqual(values, ["a", "b", "c"]);
      assert.deepEqual(await readFile(join(dir, FIRST_INDEX)), goodIndex);
    }

    test("a missing index file is rebuilt", async () => {
      const { dir, goodIndex } = await prepare();
      await rm(join(dir, FIRST_INDEX));
      await assertRepaired(dir, goodIndex);
    });

    test("an index missing its last entries is completed", async () => {
      // Like a crash after writing a batch to the .log but before its index entry.
      const { dir, goodIndex } = await prepare();
      await truncate(join(dir, FIRST_INDEX), 8); // keep only the first entry
      await assertRepaired(dir, goodIndex);
    });

    test("an index with a partly written last entry is rebuilt", async () => {
      const { dir, goodIndex } = await prepare();
      await appendFile(join(dir, FIRST_INDEX), Buffer.from([0, 0, 0])); // 3 stray bytes
      await assertRepaired(dir, goodIndex);
    });

    test("an index whose entries go backwards is rebuilt", async () => {
      const { dir, goodIndex } = await prepare();
      const bad = Buffer.from(goodIndex);
      bad.writeUInt32BE(0, 12); // second entry's position -> 0, before the first entry's
      await writeFile(join(dir, FIRST_INDEX), bad);
      await assertRepaired(dir, goodIndex);
    });

    test("an index entry pointing past the end of the .log is rebuilt", async () => {
      // Like the index write reaching disk while the .log write was lost.
      const { dir, goodIndex } = await prepare();
      const extra = Buffer.alloc(8);
      extra.writeUInt32BE(3, 0);
      extra.writeUInt32BE(9_999, 4); // far beyond the 111-byte .log
      await appendFile(join(dir, FIRST_INDEX), extra);
      await assertRepaired(dir, goodIndex);
    });
  });

  test("a missing segment in the middle is refused", async () => {
    const dir = await createTempDir();
    await (await writeFiveBatches(dir)).close();
    await rm(join(dir, "00000000000000000002.log"));
    await rm(join(dir, "00000000000000000002.index"));
    await assertCorrupt(FileLog.open(dir, { segmentMaxBytes: 100 }));
  });

  test("a log whose first segment does not start at offset 0 is refused", async () => {
    const dir = await createTempDir();
    await (await writeFiveBatches(dir)).close();
    await rm(join(dir, FIRST_LOG));
    await rm(join(dir, FIRST_INDEX));
    await assertCorrupt(FileLog.open(dir, { segmentMaxBytes: 100 }));
  });

  test("damage inside a sealed segment is detected when that data is read", async () => {
    // Opening only re-checks the LAST batch of each segment (that's what keeps
    // it fast), so damage to an earlier batch is caught by its checksum on read.
    const dir = await createTempDir();
    await (await writeFiveBatches(dir)).close();
    const path = join(dir, FIRST_LOG);
    const bytes = await readFile(path);
    bytes[36] = (bytes[36] ?? 0) ^ 0xff; // last byte of the FIRST batch in segment 0
    await writeFile(path, bytes);

    const log = await openLog(dir, { segmentMaxBytes: 100 }); // opens fine
    await assertCorrupt(log.read(0, 1)); // ...but reading the damaged batch fails
    const later = (await log.read(1, 10)).map((r) => r.value.toString());
    assert.deepEqual(later, ["b", "c", "d", "e"]); // undamaged data is still readable
  });

  test("an invalid segmentMaxBytes is rejected", async () => {
    const dir = await createTempDir();
    await assert.rejects(FileLog.open(dir, { segmentMaxBytes: 0 }), RangeError);
    await assert.rejects(FileLog.open(dir, { segmentMaxBytes: 1.5 }), RangeError);
    await assert.rejects(FileLog.open(dir, { segmentMaxBytes: 2 ** 32 }), RangeError);
  });
});
