import { describe, test } from "node:test";
import assert from "node:assert/strict";

import type { RecordInput } from "../../src/common/types.js";
import { LogError, type Log, type LogErrorCode } from "../../src/storage/log.js";

/** Creates a fresh, empty log that reads time from the given clock. */
export type LogFactory = (clock: () => number) => Promise<Log>;

function record(value: string, key: string | null = null): RecordInput {
  return { key: key === null ? null : Buffer.from(key), value: Buffer.from(value) };
}

/** Asserts that `promise` rejects with a LogError carrying exactly `code`. */
async function assertLogError(promise: Promise<unknown>, code: LogErrorCode): Promise<void> {
  await assert.rejects(promise, (err: unknown) => {
    assert.ok(err instanceof LogError, "expected a LogError");
    assert.equal(err.code, code);
    return true;
  });
}

/**
 * Behaviour every Log implementation must have. Each implementation's test
 * file calls this with its own factory, so they are all held to the same rules.
 * This file is deliberately not named *.test.ts, so the runner never runs it directly.
 */
export function runLogContractTests(name: string, createLog: LogFactory): void {
  describe(`${name} satisfies the Log contract`, () => {
    const fixedClock = () => 1_000;

    describe("append", () => {
      test("a new log is empty", async () => {
        const log = await createLog(fixedClock);
        assert.equal(log.logEndOffset, 0);
        assert.deepEqual(await log.read(0, 10), []);
      });

      test("the first record gets offset 0", async () => {
        const log = await createLog(fixedClock);
        const result = await log.append([record("a")]);
        assert.deepEqual(result, { baseOffset: 0, lastOffset: 0 });
        assert.equal(log.logEndOffset, 1);
      });

      test("a batch gets consecutive offsets", async () => {
        const log = await createLog(fixedClock);
        const result = await log.append([record("a"), record("b"), record("c")]);
        assert.deepEqual(result, { baseOffset: 0, lastOffset: 2 });
        assert.equal(log.logEndOffset, 3);
      });

      test("offsets continue across appends with no gaps", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a"), record("b")]);
        const result = await log.append([record("c")]);
        assert.deepEqual(result, { baseOffset: 2, lastOffset: 2 });
        assert.equal(log.logEndOffset, 3);
      });

      test("records in one batch share the timestamp taken at append time", async () => {
        let now = 1_000;
        const log = await createLog(() => now);
        await log.append([record("a"), record("b")]);
        now = 2_000;
        await log.append([record("c")]);
        const timestamps = (await log.read(0, 10)).map((r) => r.timestamp);
        assert.deepEqual(timestamps, [1_000, 1_000, 2_000]);
      });

      test("an empty batch is rejected and leaves the log unchanged", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a")]);
        await assertLogError(log.append([]), "EMPTY_BATCH");
        assert.equal(log.logEndOffset, 1);
      });
    });

    describe("read", () => {
      test("returns records in offset order with their keys and values", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a", "k1"), record("b", null)]);
        assert.deepEqual(await log.read(0, 10), [
          { offset: 0, timestamp: 1_000, key: Buffer.from("k1"), value: Buffer.from("a") },
          { offset: 1, timestamp: 1_000, key: null, value: Buffer.from("b") },
        ]);
      });

      test("can start from the middle of the log", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a"), record("b"), record("c")]);
        const values = (await log.read(1, 10)).map((r) => r.value.toString());
        assert.deepEqual(values, ["b", "c"]);
      });

      test("maxRecords caps how many records are returned", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a"), record("b"), record("c")]);
        const offsets = (await log.read(0, 2)).map((r) => r.offset);
        assert.deepEqual(offsets, [0, 1]);
      });

      test("reading at the log end offset returns an empty list", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a")]);
        assert.deepEqual(await log.read(1, 10), []);
      });

      test("reading past the log end offset is rejected", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a")]);
        await assertLogError(log.read(2, 10), "OFFSET_OUT_OF_RANGE");
      });

      test("negative and non-integer offsets are rejected", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a")]);
        await assertLogError(log.read(-1, 10), "INVALID_OFFSET");
        await assertLogError(log.read(0.5, 10), "INVALID_OFFSET");
        await assertLogError(log.read(Number.NaN, 10), "INVALID_OFFSET");
      });

      test("maxRecords below 1 or non-integer is rejected", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("a")]);
        await assertLogError(log.read(0, 0), "INVALID_MAX_RECORDS");
        await assertLogError(log.read(0, -1), "INVALID_MAX_RECORDS");
        await assertLogError(log.read(0, 1.5), "INVALID_MAX_RECORDS");
      });
    });

    // The log must keep its own copy of every buffer; otherwise a caller could
    // rewrite stored history by mutating a buffer it still holds.
    describe("isolation", () => {
      test("changing an input buffer after append does not change the stored record", async () => {
        const log = await createLog(fixedClock);
        const input = record("abc", "key");
        await log.append([input]);
        input.value.write("XYZ");
        input.key?.write("KEY");
        const [stored] = await log.read(0, 1);
        assert.equal(stored?.value.toString(), "abc");
        assert.equal(stored?.key?.toString(), "key");
      });

      test("changing a returned buffer does not change the stored record", async () => {
        const log = await createLog(fixedClock);
        await log.append([record("abc")]);
        const [first] = await log.read(0, 1);
        first?.value.write("XYZ");
        const [again] = await log.read(0, 1);
        assert.equal(again?.value.toString(), "abc");
      });
    });
  });
}
