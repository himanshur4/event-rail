import { crc32 } from "node:zlib";

import type { RecordInput } from "../common/types.js";

/** A batch of records that share one base offset and one timestamp. */
export interface RecordBatch {
  /** Offset of the first record; record i has offset baseOffset + i. */
  readonly baseOffset: number;
  readonly timestamp: number;
  readonly records: readonly RecordInput[];
}

/**
 * The outcome of decoding one batch.
 * - `incomplete`: the buffer ends before the batch does (e.g. a write cut short by a crash).
 * - `corrupt`: the bytes are all present but fail the checksum or are inconsistent.
 */
export type DecodeResult =
  | { readonly status: "ok"; readonly batch: RecordBatch; readonly size: number }
  | { readonly status: "incomplete" }
  | { readonly status: "corrupt"; readonly reason: string };

const BATCH_LENGTH_POS = 0;
const CRC_POS = 4;
const BASE_OFFSET_POS = 8;
const TIMESTAMP_POS = 16;
const RECORD_COUNT_POS = 24;

/** Size in bytes of the fixed part of every batch, before the first record. */
export const BATCH_HEADER_SIZE = 28;

// The checksum can't cover itself. batchLength is protected indirectly: a wrong
// length makes the checksum run over the wrong bytes.
const CRC_COVERS_FROM = BASE_OFFSET_POS;

const LENGTH_FIELD_SIZE = 4;
// -1 rather than 0, because 0 means an empty (but present) key.
const NULL_KEY_LENGTH = -1;

const MAX_UINT32 = 0xffff_ffff;
const MAX_INT32 = 0x7fff_ffff;

const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

/**
 * Encodes a batch into its on-disk byte layout.
 * Throws RangeError if the batch is empty, has invalid numbers, or is too large.
 */
export function encodeBatch(batch: RecordBatch): Buffer {
  if (batch.records.length === 0) {
    throw new RangeError("cannot encode an empty batch");
  }
  if (!Number.isSafeInteger(batch.baseOffset) || batch.baseOffset < 0) {
    throw new RangeError(`baseOffset must be a non-negative integer, got ${batch.baseOffset}`);
  }
  if (!Number.isSafeInteger(batch.timestamp)) {
    throw new RangeError(`timestamp must be an integer, got ${batch.timestamp}`);
  }

  // Size the buffer up front so it is allocated exactly once.
  let size = BATCH_HEADER_SIZE;
  for (const record of batch.records) {
    const keyLength = record.key === null ? 0 : record.key.length;
    if (keyLength > MAX_INT32) {
      throw new RangeError(`key is too large: ${keyLength} bytes`);
    }
    size += LENGTH_FIELD_SIZE + keyLength + LENGTH_FIELD_SIZE + record.value.length;
  }
  // Also guarantees every value length and the record count fit in 32 bits.
  if (size > MAX_UINT32) {
    throw new RangeError(`batch is too large: ${size} bytes`);
  }

  const buffer = Buffer.alloc(size);
  buffer.writeUInt32BE(size, BATCH_LENGTH_POS);
  buffer.writeBigUInt64BE(BigInt(batch.baseOffset), BASE_OFFSET_POS);
  buffer.writeBigInt64BE(BigInt(batch.timestamp), TIMESTAMP_POS);
  buffer.writeUInt32BE(batch.records.length, RECORD_COUNT_POS);

  let position = BATCH_HEADER_SIZE;
  for (const record of batch.records) {
    if (record.key === null) {
      buffer.writeInt32BE(NULL_KEY_LENGTH, position);
      position += LENGTH_FIELD_SIZE;
    } else {
      buffer.writeInt32BE(record.key.length, position);
      position += LENGTH_FIELD_SIZE;
      record.key.copy(buffer, position);
      position += record.key.length;
    }
    buffer.writeUInt32BE(record.value.length, position);
    position += LENGTH_FIELD_SIZE;
    record.value.copy(buffer, position);
    position += record.value.length;
  }

  // Computed last, once every covered byte is final.
  buffer.writeUInt32BE(crc32(buffer.subarray(CRC_COVERS_FROM)), CRC_POS);

  return buffer;
}

/**
 * Decodes one batch starting at `position` in `buffer`.
 * Never throws for bad data: damaged or partial bytes are reported through the
 * returned status. Throws RangeError only if `position` itself is invalid.
 */
export function decodeBatch(buffer: Buffer, position = 0): DecodeResult {
  if (!Number.isSafeInteger(position) || position < 0 || position > buffer.length) {
    throw new RangeError(`position ${position} is outside the buffer (length ${buffer.length})`);
  }

  const available = buffer.length - position;

  if (available < LENGTH_FIELD_SIZE) {
    return { status: "incomplete" };
  }
  const batchLength = buffer.readUInt32BE(position + BATCH_LENGTH_POS);
  if (batchLength < BATCH_HEADER_SIZE) {
    return corrupt(`batch length ${batchLength} is smaller than the ${BATCH_HEADER_SIZE}-byte header`);
  }
  if (available < batchLength) {
    return { status: "incomplete" };
  }

  const end = position + batchLength;

  // Verify the checksum before trusting any other field.
  const storedCrc = buffer.readUInt32BE(position + CRC_POS);
  const actualCrc = crc32(buffer.subarray(position + CRC_COVERS_FROM, end));
  if (storedCrc !== actualCrc) {
    return corrupt("checksum mismatch");
  }

  // 64-bit fields beyond Number's exact range can't be represented, so treat them as damage.
  const baseOffset = buffer.readBigUInt64BE(position + BASE_OFFSET_POS);
  const timestamp = buffer.readBigInt64BE(position + TIMESTAMP_POS);
  if (baseOffset > MAX_SAFE_BIGINT) {
    return corrupt(`base offset ${baseOffset} exceeds the largest safe integer`);
  }
  if (timestamp > MAX_SAFE_BIGINT || timestamp < MIN_SAFE_BIGINT) {
    return corrupt(`timestamp ${timestamp} is outside the safe integer range`);
  }
  const recordCount = buffer.readUInt32BE(position + RECORD_COUNT_POS);
  if (recordCount === 0) {
    return corrupt("batch contains no records");
  }

  // Every length is bounds-checked against `end` even though the checksum
  // passed, so parsing can never read past this batch.
  const records: RecordInput[] = [];
  let cursor = position + BATCH_HEADER_SIZE;
  for (let i = 0; i < recordCount; i++) {
    if (end - cursor < LENGTH_FIELD_SIZE) {
      return corrupt(`record ${i} extends past the end of the batch`);
    }
    const keyLength = buffer.readInt32BE(cursor);
    cursor += LENGTH_FIELD_SIZE;

    let key: Buffer | null;
    if (keyLength === NULL_KEY_LENGTH) {
      key = null;
    } else if (keyLength < 0) {
      return corrupt(`record ${i} has invalid key length ${keyLength}`);
    } else {
      if (end - cursor < keyLength) {
        return corrupt(`record ${i} extends past the end of the batch`);
      }
      // Copied so the record does not share memory with the input buffer.
      key = Buffer.from(buffer.subarray(cursor, cursor + keyLength));
      cursor += keyLength;
    }

    if (end - cursor < LENGTH_FIELD_SIZE) {
      return corrupt(`record ${i} extends past the end of the batch`);
    }
    const valueLength = buffer.readUInt32BE(cursor);
    cursor += LENGTH_FIELD_SIZE;
    if (end - cursor < valueLength) {
      return corrupt(`record ${i} extends past the end of the batch`);
    }
    const value = Buffer.from(buffer.subarray(cursor, cursor + valueLength));
    cursor += valueLength;

    records.push({ key, value });
  }

  // Leftover bytes mean recordCount or a length field disagrees with the data.
  if (cursor !== end) {
    return corrupt(`${end - cursor} unexpected bytes after the last record`);
  }

  return {
    status: "ok",
    batch: { baseOffset: Number(baseOffset), timestamp: Number(timestamp), records },
    size: batchLength,
  };
}

function corrupt(reason: string): DecodeResult {
  return { status: "corrupt", reason };
}
