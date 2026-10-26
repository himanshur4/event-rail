import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { crc32 } from "node:zlib";

import {
  BATCH_HEADER_SIZE,
  decodeBatch,
  encodeBatch,
  type DecodeResult,
  type RecordBatch,
} from "../../src/storage/record-batch.js";

function batchOf(records: Array<[string | null, string]>, baseOffset = 0, timestamp = 1_000): RecordBatch {
  return {
    baseOffset,
    timestamp,
    records: records.map(([key, value]) => ({
      key: key === null ? null : Buffer.from(key),
      value: Buffer.from(value),
    })),
  };
}

function decodeOk(buffer: Buffer, position = 0): Extract<DecodeResult, { status: "ok" }> {
  const result = decodeBatch(buffer, position);
  if (result.status !== "ok") {
    assert.fail(`expected ok, got ${JSON.stringify(result)}`);
  }
  return result;
}

/**
 * Applies `change` to a copy of `buffer`, then recomputes the checksum, so the
 * decoder's structural checks can be tested independently of the checksum.
 */
function tamper(buffer: Buffer, change: (copy: Buffer) => void): Buffer {
  const copy = Buffer.from(buffer);
  change(copy);
  copy.writeUInt32BE(crc32(copy.subarray(8)), 4);
  return copy;
}

describe("record batch encoding", () => {
  describe("round trip", () => {
    test("a single record with a key decodes to the same batch", () => {
      const batch = batchOf([["k1", "hello"]], 42, 1_700_000_000_000);
      const result = decodeOk(encodeBatch(batch));
      assert.deepEqual(result.batch, batch);
    });

    test("null keys, empty keys, empty values, and many records survive", () => {
      const batch = batchOf([
        [null, "no key"],
        ["", "empty key"],
        ["k", ""],
        ["k2", "x".repeat(1_000)],
      ]);
      assert.deepEqual(decodeOk(encodeBatch(batch)).batch, batch);
    });

    test("large offsets and timestamps survive", () => {
      const batch = batchOf([["k", "v"]], 2 ** 40, Number.MAX_SAFE_INTEGER);
      assert.deepEqual(decodeOk(encodeBatch(batch)).batch, batch);
    });

    test("binary values (not text) survive byte for byte", () => {
      const bytes = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
      const batch: RecordBatch = { baseOffset: 0, timestamp: 0, records: [{ key: null, value: bytes }] };
      assert.deepEqual(decodeOk(encodeBatch(batch)).batch, batch);
    });

    test("size equals the encoded length, so batches can be read back to back", () => {
      const first = encodeBatch(batchOf([["a", "1"]], 0));
      const second = encodeBatch(batchOf([["b", "2"], ["c", "3"]], 1));
      const both = Buffer.concat([first, second]);

      const one = decodeOk(both, 0);
      assert.equal(one.size, first.length);
      const two = decodeOk(both, one.size);
      assert.equal(two.batch.baseOffset, 1);
      assert.equal(two.batch.records.length, 2);
      assert.equal(one.size + two.size, both.length);
    });

    test("decoded buffers are copies, independent of the input bytes", () => {
      const encoded = encodeBatch(batchOf([["k", "abc"]]));
      const decoded = decodeOk(encoded).batch;
      encoded.fill(0);
      assert.equal(decoded.records[0]?.value.toString(), "abc");
    });
  });

  // Pins the on-disk format: a change here would make existing files unreadable.
  describe("byte layout", () => {
    test("each field is at its documented position", () => {
      const encoded = encodeBatch(batchOf([["k", "vv"]], 5, 1_000));
      // header 28 + keyLength 4 + "k" 1 + valueLength 4 + "vv" 2
      assert.equal(encoded.length, 39);
      assert.equal(encoded.readUInt32BE(0), 39);
      assert.equal(encoded.readUInt32BE(4), crc32(encoded.subarray(8)));
      assert.equal(encoded.readBigUInt64BE(8), 5n);
      assert.equal(encoded.readBigInt64BE(16), 1_000n);
      assert.equal(encoded.readUInt32BE(24), 1);
      assert.equal(encoded.readInt32BE(28), 1);
      assert.equal(encoded.subarray(32, 33).toString(), "k");
      assert.equal(encoded.readUInt32BE(33), 2);
      assert.equal(encoded.subarray(37, 39).toString(), "vv");
    });

    test("a null key is written as length -1 with no key bytes", () => {
      const encoded = encodeBatch(batchOf([[null, "v"]]));
      assert.equal(encoded.readInt32BE(BATCH_HEADER_SIZE), -1);
      assert.equal(encoded.length, 37);
    });
  });

  describe("incomplete input", () => {
    test("an empty buffer is incomplete", () => {
      assert.deepEqual(decodeBatch(Buffer.alloc(0)), { status: "incomplete" });
    });

    test("a batch cut off at any byte is incomplete, never ok or corrupt", () => {
      const encoded = encodeBatch(batchOf([["key", "value"], [null, "second"]]));
      // Simulates a crash after writing only the first `length` bytes.
      for (let length = 0; length < encoded.length; length++) {
        assert.deepEqual(
          decodeBatch(encoded.subarray(0, length)),
          { status: "incomplete" },
          `cut at ${length} bytes`,
        );
      }
    });
  });

  describe("corrupt input", () => {
    test("a damaged byte anywhere in the batch is never accepted", () => {
      const encoded = encodeBatch(batchOf([["key", "value"], [null, "second"]]));
      for (let i = 0; i < encoded.length; i++) {
        const damaged = Buffer.from(encoded);
        damaged[i] = (damaged[i] ?? 0) ^ 0xff;
        // Damage to the length field may read as incomplete rather than corrupt.
        assert.notEqual(decodeBatch(damaged).status, "ok", `byte ${i} damaged`);
      }
    });

    test("a damaged payload byte is reported as a checksum mismatch", () => {
      const damaged = encodeBatch(batchOf([["k", "value"]]));
      damaged[damaged.length - 1] = (damaged[damaged.length - 1] ?? 0) ^ 0xff;
      assert.deepEqual(decodeBatch(damaged), { status: "corrupt", reason: "checksum mismatch" });
    });

    test("a length smaller than the header is corrupt", () => {
      const damaged = encodeBatch(batchOf([["k", "v"]]));
      damaged.writeUInt32BE(10, 0);
      assert.equal(decodeBatch(damaged).status, "corrupt");
    });

    // The remaining cases keep the checksum valid, so only the structural checks can catch them.
    test("a record count larger than the records present is corrupt", () => {
      const encoded = encodeBatch(batchOf([["k", "v"]]));
      const damaged = tamper(encoded, (copy) => copy.writeUInt32BE(2, 24));
      assert.equal(decodeBatch(damaged).status, "corrupt");
    });

    test("a record count smaller than the records present is corrupt", () => {
      const encoded = encodeBatch(batchOf([["a", "1"], ["b", "2"]]));
      const damaged = tamper(encoded, (copy) => copy.writeUInt32BE(1, 24));
      assert.equal(decodeBatch(damaged).status, "corrupt");
    });

    test("a record count of zero is corrupt", () => {
      const encoded = encodeBatch(batchOf([["k", "v"]]));
      const damaged = tamper(encoded, (copy) => copy.writeUInt32BE(0, 24));
      assert.equal(decodeBatch(damaged).status, "corrupt");
    });

    test("a negative key length other than -1 is corrupt", () => {
      const encoded = encodeBatch(batchOf([["k", "v"]]));
      const damaged = tamper(encoded, (copy) => copy.writeInt32BE(-2, BATCH_HEADER_SIZE));
      assert.equal(decodeBatch(damaged).status, "corrupt");
    });

    test("a base offset beyond the safe integer range is corrupt", () => {
      const encoded = encodeBatch(batchOf([["k", "v"]]));
      const damaged = tamper(encoded, (copy) => copy.writeBigUInt64BE(2n ** 60n, 8));
      assert.equal(decodeBatch(damaged).status, "corrupt");
    });
  });

  describe("invalid arguments", () => {
    test("encoding an empty batch throws", () => {
      assert.throws(() => encodeBatch(batchOf([])), RangeError);
    });

    test("encoding a negative or fractional base offset throws", () => {
      assert.throws(() => encodeBatch(batchOf([["k", "v"]], -1)), RangeError);
      assert.throws(() => encodeBatch(batchOf([["k", "v"]], 1.5)), RangeError);
    });

    test("encoding a non-integer timestamp throws", () => {
      assert.throws(() => encodeBatch(batchOf([["k", "v"]], 0, Number.NaN)), RangeError);
    });

    test("decoding from a position outside the buffer throws", () => {
      const encoded = encodeBatch(batchOf([["k", "v"]]));
      assert.throws(() => decodeBatch(encoded, encoded.length + 1), RangeError);
      assert.throws(() => decodeBatch(encoded, -1), RangeError);
    });
  });
});
