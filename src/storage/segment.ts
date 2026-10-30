// A segment is one .log file of record batches plus its .index file.

import { appendFile, open, readFile, writeFile, type FileHandle } from "node:fs/promises";
import { join } from "node:path";

import type { LogRecord } from "../common/types.js";
import { LogError } from "./log.js";
import { decodeBatch } from "./record-batch.js";

/** Bytes per index entry: a 4-byte relative offset and a 4-byte file position. */
export const INDEX_ENTRY_SIZE = 8;

/** Largest value a 4-byte unsigned field can hold (positions and relative offsets). */
export const MAX_UINT32 = 0xffff_ffff;

const LOG_FILE_PATTERN = /^(\d{20})\.log$/;

/** The .log and .index file names for the segment starting at `baseOffset`. */
export function segmentFileNames(baseOffset: number): { log: string; index: string } {
  const name = String(baseOffset).padStart(20, "0");
  return { log: `${name}.log`, index: `${name}.index` };
}

/** Reads the base offset out of a .log file name, or returns null if it isn't one. */
export function parseSegmentFileName(fileName: string): number | null {
  const match = LOG_FILE_PATTERN.exec(fileName);
  const digits = match?.[1];
  if (digits === undefined) {
    return null;
  }
  const baseOffset = Number(digits);
  // 20 digits can exceed the safe integer range; such a name is not ours.
  return Number.isSafeInteger(baseOffset) ? baseOffset : null;
}

/** What crash recovery cut off the end of a segment's .log file when it was opened. */
export interface TailTruncation {
  readonly segmentBaseOffset: number;
  /** The .log file now ends at this byte. */
  readonly position: number;
  readonly discardedBytes: number;
  /** Why the first discarded batch was invalid. */
  readonly reason: string;
}

// Where one batch is in the .log file: the index file's contents plus each batch's size.
interface BatchLocation {
  readonly baseOffset: number; // absolute offset of the batch's first record
  readonly position: number;
  readonly size: number;
}

interface SegmentState {
  readonly batches: BatchLocation[];
  readonly size: number;
  readonly nextOffset: number;
  readonly truncation: TailTruncation | null;
}

// The valid batches found in some .log bytes and where they end. If the scan
// stopped early, `invalid` says why and whether a crash mid-append could explain it.
interface ScanResult {
  readonly batches: BatchLocation[];
  readonly nextOffset: number;
  readonly endPosition: number;
  readonly invalid: { readonly reason: string; readonly tornWritePossible: boolean } | null;
}

/** One .log file of record batches and its .index file. */
export class Segment {
  /** Offset of the first record this segment holds (also its file name). */
  readonly baseOffset: number;
  /** What crash recovery cut off the end of this segment when it was opened, or null. */
  readonly truncation: TailTruncation | null;
  private readonly logFile: FileHandle;
  private readonly indexFile: FileHandle;
  private readonly batches: BatchLocation[];
  // Entries for batches written but not yet fsynced (syncOnAppend: false only).
  // flush() writes them once the .log is durable.
  private readonly pendingIndexEntries: Buffer[] = [];
  private sizeBytes: number;
  private nextOffsetValue: number;

  // Private: opening needs `await`, so callers use Segment.open().
  private constructor(baseOffset: number, logFile: FileHandle, indexFile: FileHandle, state: SegmentState) {
    this.baseOffset = baseOffset;
    this.logFile = logFile;
    this.indexFile = indexFile;
    this.batches = state.batches;
    this.sizeBytes = state.size;
    this.nextOffsetValue = state.nextOffset;
    this.truncation = state.truncation;
  }

  /**
   * Opens (or creates) the segment starting at `baseOffset` in `dir`, repairing
   * its index from the .log file if needed. With `recoverTail` (only for the
   * active segment), a torn write at the end of the .log is cut off. Rejects
   * with CORRUPT_LOG if the .log is damaged in any other way.
   */
  static async open(dir: string, baseOffset: number, recoverTail: boolean): Promise<Segment> {
    const names = segmentFileNames(baseOffset);
    const logPath = join(dir, names.log);
    const indexPath = join(dir, names.index);
    const logFile = await open(logPath, "a+");
    let indexFile: FileHandle | undefined;
    try {
      // Repairs write by path before the index handle is opened: on Windows a
      // file open in append mode can't be truncated.
      const state = await loadSegment(logFile, logPath, indexPath, baseOffset, recoverTail);
      indexFile = await open(indexPath, "a");
      return new Segment(baseOffset, logFile, indexFile, state);
    } catch (error) {
      await indexFile?.close();
      await logFile.close();
      throw error;
    }
  }

  /** Bytes in the .log file. */
  get size(): number {
    return this.sizeBytes;
  }

  /** The offset the next batch appended to this segment would receive. */
  get nextOffset(): number {
    return this.nextOffsetValue;
  }

  /** True if the segment holds no batches yet. */
  get isEmpty(): boolean {
    return this.batches.length === 0;
  }

  /**
   * Appends one already-encoded batch. The caller (FileLog) guarantees appends
   * are never concurrent and that `baseOffset` equals this.nextOffset.
   */
  async append(bytes: Buffer, baseOffset: number, recordCount: number, sync: boolean): Promise<void> {
    const position = this.sizeBytes;
    const entry = encodeIndexEntry(baseOffset - this.baseOffset, position);

    await writeAll(this.logFile, bytes);

    // The index may only list durable batches: crash recovery treats every
    // listed batch as acknowledged and never cuts it off. So an entry is written
    // only after its batch is fsynced. The index itself is not fsynced; lost
    // entries are restored from the .log on open.
    if (sync) {
      await this.logFile.sync();
      await writeAll(this.indexFile, entry);
    } else {
      this.pendingIndexEntries.push(entry);
    }

    // Updated last, so readers never see a batch that isn't fully written.
    this.batches.push({ baseOffset, position, size: bytes.length });
    this.sizeBytes += bytes.length;
    this.nextOffsetValue = baseOffset + recordCount;
  }

  /**
   * Makes every appended batch durable, then writes the index entries that were
   * waiting for that. Does nothing if no entries are waiting.
   */
  async flush(): Promise<void> {
    if (this.pendingIndexEntries.length === 0) {
      return;
    }
    await this.logFile.sync();
    await writeAll(this.indexFile, Buffer.concat(this.pendingIndexEntries.splice(0)));
  }

  /**
   * Reads up to `maxRecords` records starting at `offset`, from this segment
   * only. Returns fewer (possibly none) if the segment ends first.
   */
  async read(offset: number, maxRecords: number): Promise<LogRecord[]> {
    const result: LogRecord[] = [];
    if (offset >= this.nextOffsetValue || this.batches.length === 0) {
      return result;
    }

    for (let i = this.findBatchIndex(offset); i < this.batches.length; i++) {
      if (result.length >= maxRecords) {
        break;
      }
      const location = this.batches[i];
      if (location === undefined) {
        break;
      }

      const bytes = await readExactly(this.logFile, location.position, location.size);
      const decoded = decodeBatch(bytes);
      if (decoded.status !== "ok") {
        // Opening doesn't re-check every batch of a sealed segment, so damage
        // inside one is first noticed here.
        throw new LogError(
          "CORRUPT_LOG",
          `segment ${this.baseOffset}: batch at byte ${location.position} is ${decoded.status}`,
        );
      }

      const { baseOffset, timestamp, records } = decoded.batch;
      for (const [index, record] of records.entries()) {
        const recordOffset = baseOffset + index;
        if (recordOffset < offset) {
          continue; // the first batch may start before `offset`
        }
        if (result.length >= maxRecords) {
          break;
        }
        result.push({ offset: recordOffset, timestamp, key: record.key, value: record.value });
      }
    }
    return result;
  }

  /**
   * Closes both files. Does not flush: after a failed write the log must not
   * fsync again, so FileLog flushes explicitly before a clean close.
   */
  async close(): Promise<void> {
    await Promise.all([this.logFile.close(), this.indexFile.close()]);
  }

  // Binary search: index of the last batch whose baseOffset is <= offset.
  // Assumes at least one batch and offset >= this.baseOffset.
  private findBatchIndex(offset: number): number {
    let low = 0;
    let high = this.batches.length - 1;
    while (low < high) {
      // Rounded up so `low = mid` always makes progress.
      const mid = Math.ceil((low + high) / 2);
      const midBaseOffset = this.batches[mid]?.baseOffset ?? 0;
      if (midBaseOffset <= offset) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }
}

// Crash recovery: a crash during an append can leave a torn write at the end of
// the .log, either part of a batch or reserved bytes that never received their
// data. Cutting it off is safe because an append is acknowledged only after its
// batch is fsynced, so a torn batch was never acknowledged. Damage to
// acknowledged data must never be cut off, so the tail is cut only if:
//   1. it is in the active segment (sealed segments are flushed before the log
//      moves past them, so a crash can't tear them);
//   2. the bytes are incomplete or corrupt (a crash can't produce a valid batch
//      with the wrong offset);
//   3. the index lists nothing at or after the cut (it only lists durable batches).
// Otherwise opening fails with CORRUPT_LOG and no file is modified.
//
// Loading trusts the index but verifies its tail. If the index is malformed or
// that check fails, the whole .log is scanned and the rules above decide what
// happens to an invalid tail. Batches before the last indexed one are not
// re-checked on open; their checksums are verified on read.
async function loadSegment(
  logFile: FileHandle,
  logPath: string,
  indexPath: string,
  segmentBaseOffset: number,
  recoverTail: boolean,
): Promise<SegmentState> {
  const logSize = (await logFile.stat()).size;
  const indexBytes = await readIndexFile(indexPath);

  const indexed = parseIndex(indexBytes, segmentBaseOffset, logSize);
  if (indexed !== null) {
    const last = indexed.at(-1);
    const startPosition = last?.position ?? 0;
    const startOffset = last?.baseOffset ?? segmentBaseOffset;
    const tailBytes = await readExactly(logFile, startPosition, logSize - startPosition);
    const tail = scanBatches(tailBytes, startPosition, startOffset);

    if (tail.invalid === null) {
      // Batches are stored back to back, so each size is the gap to the next position.
      const earlier: BatchLocation[] = indexed.slice(0, -1).map((entry, i) => ({
        baseOffset: entry.baseOffset,
        position: entry.position,
        size: (indexed[i + 1]?.position ?? entry.position) - entry.position,
      }));
      // The scan starts at the last indexed batch; anything after it is missing
      // from the index. With an empty index, every scanned batch is missing.
      const missing = tail.batches.slice(last === undefined ? 0 : 1);
      if (missing.length > 0) {
        // After a crash these may exist only in the page cache, so they are
        // made durable before the index lists them.
        await logFile.sync();
        await appendFile(indexPath, encodeIndexEntries(missing, segmentBaseOffset));
      }
      return {
        batches: [...earlier, ...tail.batches],
        size: logSize,
        nextOffset: tail.nextOffset,
        truncation: null,
      };
    }
  }

  const full = scanBatches(await readExactly(logFile, 0, logSize), 0, segmentBaseOffset);
  let truncation: TailTruncation | null = null;
  if (full.invalid !== null) {
    assertTornWrite(full.invalid, full.endPosition, indexBytes, segmentBaseOffset, recoverTail);
    await truncateFile(logPath, full.endPosition);
    truncation = {
      segmentBaseOffset,
      position: full.endPosition,
      discardedBytes: logSize - full.endPosition,
      reason: full.invalid.reason,
    };
  }

  // The rebuilt index may only list durable batches.
  await logFile.sync();
  await writeFile(indexPath, encodeIndexEntries(full.batches, segmentBaseOffset));
  return {
    batches: full.batches,
    size: full.endPosition,
    nextOffset: full.nextOffset,
    truncation,
  };
}

// Throws CORRUPT_LOG, naming the rule that failed, unless the invalid bytes from
// `cutPosition` onwards may be cut off as a torn write (rules above loadSegment).
function assertTornWrite(
  invalid: { readonly reason: string; readonly tornWritePossible: boolean },
  cutPosition: number,
  indexBytes: Buffer,
  segmentBaseOffset: number,
  recoverTail: boolean,
): void {
  const problem = `segment ${segmentBaseOffset}: ${invalid.reason}`;
  // Rule 1
  if (!recoverTail) {
    throw new LogError("CORRUPT_LOG", `${problem}; sealed segments are never cut short`);
  }
  // Rule 2
  if (!invalid.tornWritePossible) {
    throw new LogError("CORRUPT_LOG", problem);
  }
  // Rule 3
  if (indexListsPositionAtOrAfter(indexBytes, cutPosition)) {
    throw new LogError(
      "CORRUPT_LOG",
      `${problem}, but the index shows data from that point was completely written`,
    );
  }
}

// Reads the raw entries even if parseIndex rejected the index: for this safety
// check, any sign of durable data at or after `position` is reason to refuse.
function indexListsPositionAtOrAfter(indexBytes: Buffer, position: number): boolean {
  // A partly written last entry is ignored.
  const completeBytes = indexBytes.length - (indexBytes.length % INDEX_ENTRY_SIZE);
  for (let at = 0; at < completeBytes; at += INDEX_ENTRY_SIZE) {
    if (indexBytes.readUInt32BE(at + 4) >= position) {
      return true;
    }
  }
  return false;
}

// Fsynced so the cut is durable before anything is appended after it. Uses its
// own "r+" handle because Windows refuses to truncate through an append-mode one.
async function truncateFile(path: string, length: number): Promise<void> {
  const file = await open(path, "r+");
  try {
    await file.truncate(length);
    await file.sync();
  } finally {
    await file.close();
  }
}

// A missing index file (new segment, or deleted) is treated as empty and rebuilt.
async function readIndexFile(indexPath: string): Promise<Buffer> {
  try {
    return await readFile(indexPath);
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return Buffer.alloc(0);
    }
    throw error;
  }
}

// Returns the index entries, or null if the index can't be trusted.
function parseIndex(
  bytes: Buffer,
  segmentBaseOffset: number,
  logSize: number,
): Array<{ baseOffset: number; position: number }> | null {
  // A partial last entry: the index write was cut short.
  if (bytes.length % INDEX_ENTRY_SIZE !== 0) {
    return null;
  }
  const entries: Array<{ baseOffset: number; position: number }> = [];
  for (let at = 0; at < bytes.length; at += INDEX_ENTRY_SIZE) {
    const relativeOffset = bytes.readUInt32BE(at);
    const position = bytes.readUInt32BE(at + 4);
    const previous = entries.at(-1);
    if (previous === undefined) {
      // The first batch always starts at byte 0 with the segment's base offset.
      if (relativeOffset !== 0 || position !== 0) {
        return null;
      }
    } else if (
      // Binary search needs offsets and positions to strictly increase.
      segmentBaseOffset + relativeOffset <= previous.baseOffset ||
      position <= previous.position
    ) {
      return null;
    }
    // Points at data the .log doesn't have (e.g. the .log write was lost).
    if (position >= logSize) {
      return null;
    }
    entries.push({ baseOffset: segmentBaseOffset + relativeOffset, position });
  }
  return entries;
}

// Decodes batches from `bytes`, which begin at byte `startPosition` of the .log,
// checking that each continues exactly where the previous one stopped. Stops at
// the first invalid batch and returns the valid ones before it.
function scanBatches(bytes: Buffer, startPosition: number, expectedBaseOffset: number): ScanResult {
  const batches: BatchLocation[] = [];
  let position = 0; // within `bytes`, not the file
  let nextOffset = expectedBaseOffset;
  while (position < bytes.length) {
    const result = decodeBatch(bytes, position);
    const filePosition = startPosition + position;
    if (result.status !== "ok") {
      const detail = result.status === "corrupt" ? `corrupt (${result.reason})` : "incomplete";
      return {
        batches,
        nextOffset,
        endPosition: filePosition,
        invalid: { reason: `batch at byte ${filePosition} is ${detail}`, tornWritePossible: true },
      };
    }
    if (result.batch.baseOffset !== nextOffset) {
      // Intact (valid checksum) but out of place, e.g. copied from another log.
      return {
        batches,
        nextOffset,
        endPosition: filePosition,
        invalid: {
          reason: `batch at byte ${filePosition} has base offset ${result.batch.baseOffset}, expected ${nextOffset}`,
          tornWritePossible: false,
        },
      };
    }
    batches.push({ baseOffset: nextOffset, position: filePosition, size: result.size });
    position += result.size;
    nextOffset += result.batch.records.length;
  }
  return { batches, nextOffset, endPosition: startPosition + position, invalid: null };
}

function encodeIndexEntry(relativeOffset: number, position: number): Buffer {
  const entry = Buffer.alloc(INDEX_ENTRY_SIZE);
  entry.writeUInt32BE(relativeOffset, 0);
  entry.writeUInt32BE(position, 4);
  return entry;
}

// Joined into one Buffer so a repair writes all entries in one operation.
function encodeIndexEntries(batches: readonly BatchLocation[], segmentBaseOffset: number): Buffer {
  return Buffer.concat(batches.map((b) => encodeIndexEntry(b.baseOffset - segmentBaseOffset, b.position)));
}

// file.read() may return fewer bytes than requested, so keep reading until full.
async function readExactly(file: FileHandle, position: number, length: number): Promise<Buffer> {
  const buffer = Buffer.alloc(length);
  let filled = 0;
  while (filled < length) {
    const { bytesRead } = await file.read(buffer, filled, length - filled, position + filled);
    if (bytesRead === 0) {
      throw new LogError("CORRUPT_LOG", `file ended at byte ${position + filled}, expected more data`);
    }
    filled += bytesRead;
  }
  return buffer;
}

// file.write() may write fewer bytes than requested, so loop until done.
// Position null = the current position, which in append mode is the end.
async function writeAll(file: FileHandle, bytes: Buffer): Promise<void> {
  let written = 0;
  while (written < bytes.length) {
    const { bytesWritten } = await file.write(bytes, written, bytes.length - written, null);
    written += bytesWritten;
  }
}
