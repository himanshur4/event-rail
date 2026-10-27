// A segment is one .log file of record batches plus its .index file. Only the
// newest (active) segment is written to; older segments are sealed and never
// change, so old data can later be deleted one whole file at a time.
//
// Each file is named after its base offset, padded to 20 digits so that name
// order equals offset order:
//
//   00000000000000000000.log / .index   offsets 0..999     (sealed)
//   00000000000000001000.log / .index   offsets 1000..     (active)
//
// The .index file holds one 8-byte entry per batch:
//
//   ┌──────────────────────────┬────────────────────────────────┐
//   │ relative offset (uint32) │ byte position in .log (uint32) │
//   └──────────────────────────┴────────────────────────────────┘
//   relative offset = batch base offset − segment base offset
//
// The .log is the source of truth; the index only lets a segment open without
// decoding its whole .log. It can always be rebuilt from the .log, which is why
// it is never fsynced.

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
}

type ScanResult =
  | { readonly ok: true; readonly batches: BatchLocation[]; readonly nextOffset: number }
  | { readonly ok: false; readonly reason: string };

/** One .log file of record batches and its .index file. */
export class Segment {
  /** Offset of the first record this segment holds (also its file name). */
  readonly baseOffset: number;

  private readonly logFile: FileHandle;
  private readonly indexFile: FileHandle;
  private readonly batches: BatchLocation[];
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
  }

  /**
   * Opens (or creates) the segment starting at `baseOffset` in `dir`, repairing
   * its index from the .log file if needed. Rejects with CORRUPT_LOG if the
   * .log file itself is damaged.
   */
  static async open(dir: string, baseOffset: number): Promise<Segment> {
    const names = segmentFileNames(baseOffset);
    const indexPath = join(dir, names.index);
    const logFile = await open(join(dir, names.log), "a+");
    let indexFile: FileHandle | undefined;
    try {
      // The index is loaded and repaired before its handle is opened: on Windows
      // a file open in append mode can't be truncated, so repairs write by path.
      const state = await loadSegment(logFile, indexPath, baseOffset);
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

    await writeAll(this.logFile, bytes);
    if (sync) {
      await this.logFile.sync();
    }

    // Written after the .log, so a crash in between leaves the index one entry
    // short, which opening the segment repairs. Not fsynced for the same reason.
    await writeAll(this.indexFile, encodeIndexEntry(baseOffset - this.baseOffset, position));

    // Updated last, so readers never see a batch that isn't fully written.
    this.batches.push({ baseOffset, position, size: bytes.length });
    this.sizeBytes += bytes.length;
    this.nextOffsetValue = baseOffset + recordCount;
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

  /** Closes both files. */
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

// Loading a segment: trust the index, but verify its tail.
//
//   .index says:  [batch @0] [batch @58] [batch @103]          (3 entries)
//   .log holds:   [batch @0] [batch @58] [batch @103] [batch @148]
//                                         ▲
//                         re-decode from the last indexed batch to the end:
//                         confirms it is intact, and finds batch @148, which
//                         the index is missing (crash between the two writes)
//
// If the index is malformed, or that re-check fails, it is rebuilt by scanning
// the whole .log. If even that scan fails, the .log is damaged: CORRUPT_LOG.
//
// Trade-off: batches before the last indexed one are not re-checked on open,
// which would mean reading the whole file. Their checksums are verified on read.
async function loadSegment(
  logFile: FileHandle,
  indexPath: string,
  segmentBaseOffset: number,
): Promise<SegmentState> {
  const logSize = (await logFile.stat()).size;
  const indexBytes = await readIndexFile(indexPath);

  const indexed = parseIndex(indexBytes, segmentBaseOffset, logSize);
  if (indexed !== null) {
    const last = indexed.at(-1);
    const startPosition = last?.position ?? 0;
    const startOffset = last?.baseOffset ?? segmentBaseOffset;
    const tail = await scanLog(logFile, startPosition, logSize, startOffset);

    if (tail.ok) {
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
        await appendFile(indexPath, encodeIndexEntries(missing, segmentBaseOffset));
      }
      return { batches: [...earlier, ...tail.batches], size: logSize, nextOffset: tail.nextOffset };
    }
  }

  const full = await scanLog(logFile, 0, logSize, segmentBaseOffset);
  if (!full.ok) {
    throw new LogError("CORRUPT_LOG", `segment ${segmentBaseOffset}: ${full.reason}`);
  }
  await writeFile(indexPath, encodeIndexEntries(full.batches, segmentBaseOffset));
  return { batches: full.batches, size: logSize, nextOffset: full.nextOffset };
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

// Decodes batches from `fromPosition` to the end of the .log file, checking
// that each one continues exactly where the previous one stopped.
async function scanLog(
  logFile: FileHandle,
  fromPosition: number,
  logSize: number,
  expectedBaseOffset: number,
): Promise<ScanResult> {
  const contents = await readExactly(logFile, fromPosition, logSize - fromPosition);
  const batches: BatchLocation[] = [];
  let position = 0; // within `contents`, not the file
  let nextOffset = expectedBaseOffset;
  while (position < contents.length) {
    const result = decodeBatch(contents, position);
    const filePosition = fromPosition + position;
    if (result.status !== "ok") {
      const detail = result.status === "corrupt" ? `corrupt (${result.reason})` : "incomplete";
      return { ok: false, reason: `batch at byte ${filePosition} is ${detail}` };
    }
    if (result.batch.baseOffset !== nextOffset) {
      return {
        ok: false,
        reason: `batch at byte ${filePosition} has base offset ${result.batch.baseOffset}, expected ${nextOffset}`,
      };
    }
    batches.push({ baseOffset: nextOffset, position: filePosition, size: result.size });
    position += result.size;
    nextOffset += result.batch.records.length;
  }
  return { ok: true, batches, nextOffset };
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
