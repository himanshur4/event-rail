// A Log stored on disk as a series of segments in one directory (see segment.ts
// for the file formats). The last segment is active and receives all appends.
//
// Durability: by default every append fsyncs the .log before it resolves, so
// an acknowledged append survives power loss, not just a process crash.
//
// Current limits:
// - A damaged .log file is refused by open(); repairing it is crash recovery (Milestone 3).
// - New segment files are not made durable at the directory level (fsync of
//   the directory itself), which some file systems need after creating a file.
// - Every segment keeps its files open, so very many segments mean many open handles.
// - Only one process may use a directory at a time; this is not enforced.

import { mkdir, readdir } from "node:fs/promises";

import type { LogRecord, RecordInput } from "../common/types.js";
import { LogError, validateReadArguments, type AppendResult, type Log } from "./log.js";
import { encodeBatch } from "./record-batch.js";
import { MAX_UINT32, Segment, parseSegmentFileName } from "./segment.js";

export interface FileLogOptions {
  /** Returns the current time in milliseconds. Injectable so tests control timestamps. */
  readonly clock?: () => number;
  /**
   * Whether every append waits for fsync (default: true). When false, appended
   * data survives a crash of this process but may be lost on power failure.
   */
  readonly syncOnAppend?: boolean;
  /**
   * Size in bytes at which the active segment is closed to new batches and a
   * new segment is started (default: 16 MiB). A single batch larger than this
   * is stored alone in its own segment.
   */
  readonly segmentMaxBytes?: number;
}

// Large enough that segments roll rarely, small enough that rebuilding one
// segment's index (a full scan) stays quick.
const DEFAULT_SEGMENT_MAX_BYTES = 16 * 1024 * 1024;

/** A Log stored as a series of append-only segment files in one directory. */
export class FileLog implements Log {
  private readonly dir: string;
  private readonly clock: () => number;
  private readonly syncOnAppend: boolean;
  private readonly segmentMaxBytes: number;
  // Oldest first; the last one is active. Invariant: never empty, and each
  // segment's baseOffset equals the previous segment's nextOffset.
  private readonly segments: Segment[];
  private closed = false;
  private appendQueue: Promise<void> = Promise.resolve();

  // Private: opening needs `await`, so callers use FileLog.open().
  private constructor(dir: string, segments: Segment[], options: FileLogOptions, segmentMaxBytes: number) {
    this.dir = dir;
    this.segments = segments;
    this.clock = options.clock ?? (() => Date.now());
    this.syncOnAppend = options.syncOnAppend ?? true;
    this.segmentMaxBytes = segmentMaxBytes;
  }

  /**
   * Opens (or creates) the log stored in `dir`. Rejects with CORRUPT_LOG if a
   * segment's .log file is damaged or the segments don't fit together.
   * Throws RangeError for an invalid segmentMaxBytes.
   */
  static async open(dir: string, options: FileLogOptions = {}): Promise<FileLog> {
    const segmentMaxBytes = options.segmentMaxBytes ?? DEFAULT_SEGMENT_MAX_BYTES;
    // Index entries store positions in 4 bytes, so a segment must stay below 4 GiB.
    if (!Number.isSafeInteger(segmentMaxBytes) || segmentMaxBytes < 1 || segmentMaxBytes > MAX_UINT32) {
      throw new RangeError(`segmentMaxBytes must be between 1 and ${MAX_UINT32}, got ${segmentMaxBytes}`);
    }

    await mkdir(dir, { recursive: true });

    // readdir order is unspecified, so sort numerically.
    const baseOffsets = (await readdir(dir))
      .map(parseSegmentFileName)
      .filter((o): o is number => o !== null)
      .sort((a, b) => a - b);
    if (baseOffsets.length === 0) {
      baseOffsets.push(0);
    }

    const segments: Segment[] = [];
    try {
      for (const baseOffset of baseOffsets) {
        const previous = segments.at(-1);
        // The log starts at offset 0 (old segments are never deleted yet), and
        // each segment must start exactly where the previous one ended.
        const expected = previous === undefined ? 0 : previous.nextOffset;
        if (baseOffset !== expected) {
          throw new LogError(
            "CORRUPT_LOG",
            `segment ${baseOffset} does not follow on from offset ${expected} (missing or extra segment file)`,
          );
        }
        segments.push(await Segment.open(dir, baseOffset));
      }
      return new FileLog(dir, segments, options, segmentMaxBytes);
    } catch (error) {
      await Promise.all(segments.map((s) => s.close()));
      throw error;
    }
  }

  get logEndOffset(): number {
    return this.activeSegment.nextOffset;
  }

  async append(records: readonly RecordInput[]): Promise<AppendResult> {
    this.ensureOpen();
    if (records.length === 0) {
      throw new LogError("EMPTY_BATCH", "cannot append an empty batch");
    }

    // Copied now: the write may happen later, after earlier appends finish, and
    // the caller could change its buffers meanwhile.
    const copies: RecordInput[] = records.map((r) => ({
      key: r.key === null ? null : Buffer.from(r.key),
      value: Buffer.from(r.value),
    }));

    // appendNow() awaits between reading logEndOffset and updating it, so two
    // interleaved appends could claim the same offsets. Chaining each append
    // onto the previous one runs them strictly one at a time, in call order.
    const result = this.appendQueue.then(() => this.appendNow(copies));
    // Continue after this append whether it succeeds or fails, so one failure
    // doesn't reject every append queued behind it.
    this.appendQueue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  async read(offset: number, maxRecords: number): Promise<LogRecord[]> {
    this.ensureOpen();
    validateReadArguments(offset, maxRecords, this.logEndOffset);

    const result: LogRecord[] = [];
    for (let i = this.findSegmentIndex(offset); i < this.segments.length; i++) {
      if (result.length >= maxRecords) {
        break;
      }
      const segment = this.segments[i];
      if (segment === undefined) {
        break;
      }
      const from = Math.max(offset, segment.baseOffset);
      const records = await segment.read(from, maxRecords - result.length);
      result.push(...records);
    }
    return result;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    // Refuse queued appends that haven't started, but let the current one
    // finish before closing its files.
    this.closed = true;
    await this.appendQueue;
    await Promise.all(this.segments.map((s) => s.close()));
  }

  private get activeSegment(): Segment {
    const active = this.segments.at(-1);
    if (active === undefined) {
      throw new Error("FileLog has no segments");
    }
    return active;
  }

  // Only ever called through the append queue.
  private async appendNow(records: readonly RecordInput[]): Promise<AppendResult> {
    this.ensureOpen(); // the log may have been closed while this append waited

    const baseOffset = this.logEndOffset;
    const bytes = encodeBatch({ baseOffset, timestamp: this.clock(), records });

    try {
      let segment = this.activeSegment;
      // Roll to a new segment if this batch would push the active one past the
      // limit. An empty segment always accepts the batch, so an oversized batch
      // is stored alone instead of rolling forever.
      if (!segment.isEmpty && segment.size + bytes.length > this.segmentMaxBytes) {
        segment = await Segment.open(this.dir, baseOffset);
        this.segments.push(segment);
      }
      await segment.append(bytes, baseOffset, records.length, this.syncOnAppend);
    } catch (error) {
      // A failed write or fsync may have left part of a batch on disk, and we
      // can't know how much, so the log stops accepting work. Reopening deals
      // with the damaged tail (crash recovery, Milestone 3).
      this.closed = true;
      await Promise.all(this.segments.map((s) => s.close().catch(() => undefined)));
      throw error;
    }

    return { baseOffset, lastOffset: baseOffset + records.length - 1 };
  }

  // Binary search: index of the last segment whose baseOffset is <= offset.
  // The first segment starts at 0, so any valid offset has a match.
  private findSegmentIndex(offset: number): number {
    let low = 0;
    let high = this.segments.length - 1;
    while (low < high) {
      const mid = Math.ceil((low + high) / 2);
      const midBaseOffset = this.segments[mid]?.baseOffset ?? 0;
      if (midBaseOffset <= offset) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    return low;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new LogError("LOG_CLOSED", "log is closed");
    }
  }
}
