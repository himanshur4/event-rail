// Chooses the partition a record belongs to. A record with a key goes to
// hash(key) % partitionCount, so all records for one key share a partition and
// stay in order; records without a key are spread round-robin.
//
// The hash is MurmurHash3 (x86_32, seed 0) over the key's bytes, and must never
// change: existing keys would map to different partitions. Tests pin it to the
// published reference values. A simpler hash such as FNV-1a is not enough:
// `%` mostly depends on the hash's low bits, and FNV-1a's low bits ignore the
// high bits of each input byte. MurmurHash3's finalizer mixes every input bit
// into every output bit.

const C1 = 0xcc9e2d51;
const C2 = 0x1b873593;

/**
 * The 32-bit MurmurHash3 (x86_32, seed 0) of `key`, as an unsigned integer.
 * Stable across processes, machines, and versions.
 */
export function hashKey(key: Buffer): number {
  let h = 0;

  // Body: 4-byte blocks, read little-endian as the algorithm specifies.
  // Math.imul keeps the low 32 bits of each product, which plain `*` would
  // lose above 2^53.
  const blockCount = Math.floor(key.length / 4);
  for (let i = 0; i < blockCount; i++) {
    let k = key.readUInt32LE(i * 4);
    k = Math.imul(k, C1);
    k = rotl(k, 15);
    k = Math.imul(k, C2);
    h ^= k;
    h = rotl(h, 13);
    h = (Math.imul(h, 5) + 0xe6546b64) | 0;
  }

  // Tail: the remaining 0-3 bytes.
  const tailStart = blockCount * 4;
  const tailLength = key.length - tailStart;
  if (tailLength > 0) {
    let k = 0;
    if (tailLength >= 3) {
      k ^= (key[tailStart + 2] ?? 0) << 16;
    }
    if (tailLength >= 2) {
      k ^= (key[tailStart + 1] ?? 0) << 8;
    }
    k ^= key[tailStart] ?? 0;
    k = Math.imul(k, C1);
    k = rotl(k, 15);
    k = Math.imul(k, C2);
    h ^= k;
  }

  // Finalization (fmix32): spreads every input bit across the whole hash.
  h ^= key.length;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;

  return h >>> 0; // as an unsigned 32-bit integer
}

/** The partition, from 0 to partitionCount - 1, that records with `key` belong to. */
export function partitionForKey(key: Buffer, partitionCount: number): number {
  validatePartitionCount(partitionCount);
  return hashKey(key) % partitionCount;
}

/**
 * Chooses a partition for each record: by key hash when the record has a key,
 * otherwise round-robin. One instance belongs to one producer.
 */
export class Partitioner {
  private nextRoundRobin = 0;

  /** The partition for a record with this key (or with no key, if null). */
  partition(key: Buffer | null, partitionCount: number): number {
    if (key !== null) {
      return partitionForKey(key, partitionCount);
    }
    validatePartitionCount(partitionCount);
    const partition = this.nextRoundRobin % partitionCount;
    this.nextRoundRobin += 1;
    return partition;
  }
}

// Rotates the 32 bits of `x` left by `n` places.
function rotl(x: number, n: number): number {
  return (x << n) | (x >>> (32 - n));
}

function validatePartitionCount(partitionCount: number): void {
  if (!Number.isSafeInteger(partitionCount) || partitionCount < 1) {
    throw new RangeError(`partitionCount must be a positive integer, got ${partitionCount}`);
  }
}
