import { describe, test } from "node:test";
import assert from "node:assert/strict";

import { Partitioner, hashKey, partitionForKey } from "../../src/topic/partitioner.js";

describe("hashKey", () => {
  test("matches the published MurmurHash3 (x86_32, seed 0) values", () => {
    assert.equal(hashKey(Buffer.from("")), 0x00000000);
    assert.equal(hashKey(Buffer.from("hello")), 0x248bfa47);
    assert.equal(hashKey(Buffer.from("The quick brown fox jumps over the lazy dog")), 0x2e4ff723);
  });

  test("depends only on the key's bytes", () => {
    assert.equal(hashKey(Buffer.from("a")), hashKey(Buffer.from([0x61])));
  });

  test("returns an unsigned 32-bit integer for every key length", () => {
    // Lengths 0-12 cover every mix of whole 4-byte blocks and 0-3 tail bytes.
    for (let length = 0; length <= 12; length++) {
      const hash = hashKey(Buffer.alloc(length, 0xab));
      assert.ok(Number.isInteger(hash) && hash >= 0 && hash <= 0xffffffff, `length ${length}`);
    }
  });
});

describe("partitionForKey", () => {
  test("the same key always maps to the same partition", () => {
    const first = partitionForKey(Buffer.from("user-42"), 6);
    for (let i = 0; i < 100; i++) {
      assert.equal(partitionForKey(Buffer.from("user-42"), 6), first);
    }
  });

  test("always returns a partition from 0 to partitionCount - 1", () => {
    for (let partitionCount = 1; partitionCount <= 16; partitionCount++) {
      for (let i = 0; i < 200; i++) {
        const partition = partitionForKey(Buffer.from(`key-${i}`), partitionCount);
        assert.ok(partition >= 0 && partition < partitionCount, `count ${partitionCount}, key-${i}`);
      }
    }
  });

  test("spreads distinct keys evenly across partitions", () => {
    // A perfect spread is 1,250 keys per partition. The hash is deterministic,
    // so the counts are the same on every run.
    const counts: number[] = new Array(8).fill(0);
    for (let i = 0; i < 10_000; i++) {
      const partition = partitionForKey(Buffer.from(`user-${i}`), 8);
      counts[partition] = (counts[partition] ?? 0) + 1;
    }
    for (const [partition, count] of counts.entries()) {
      assert.ok(count >= 1_100 && count <= 1_400, `partition ${partition} got ${count} keys`);
    }
  });

  test("keys that differ in a single bit don't systematically share a partition", () => {
    // Pairs of 1-byte keys differing only in bit 4 (e.g. "a" and "q"). A
    // well-mixed hash puts a pair in the same one of 8 partitions about 1 time
    // in 8 (~32 of 256); FNV-1a, whose low bits ignore higher input bits, would
    // put all 256 pairs together.
    let sharing = 0;
    for (let byte = 0; byte < 256; byte++) {
      const a = partitionForKey(Buffer.from([byte]), 8);
      const b = partitionForKey(Buffer.from([byte ^ 0x10]), 8);
      if (a === b) {
        sharing += 1;
      }
    }
    assert.ok(sharing <= 64, `${sharing} of 256 pairs shared a partition`);
  });

  test("a single partition always gets everything", () => {
    assert.equal(partitionForKey(Buffer.from("anything"), 1), 0);
  });

  test("an invalid partition count throws", () => {
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      assert.throws(() => partitionForKey(Buffer.from("k"), bad), RangeError, `count ${bad}`);
    }
  });
});

describe("Partitioner", () => {
  test("records without a key go round-robin", () => {
    const partitioner = new Partitioner();
    const chosen = Array.from({ length: 7 }, () => partitioner.partition(null, 3));
    assert.deepEqual(chosen, [0, 1, 2, 0, 1, 2, 0]);
  });

  test("records with a key use the key's hash, not the round-robin turn", () => {
    const partitioner = new Partitioner();
    const expected = partitionForKey(Buffer.from("user-42"), 3);
    partitioner.partition(null, 3); // advance the round-robin counter
    assert.equal(partitioner.partition(Buffer.from("user-42"), 3), expected);
    assert.equal(new Partitioner().partition(Buffer.from("user-42"), 3), expected);
  });

  test("an invalid partition count throws for keyless records too", () => {
    assert.throws(() => new Partitioner().partition(null, 0), RangeError);
  });
});
