// Consumer groups' committed offsets: for each (group, topic, partition), the next
// offset the group will read. Every commit is appended as one record to a FileLog
// of its own, and the latest record for a key is the current value. On open, the
// log is replayed from offset 0 to rebuild the in-memory map.
//
// A log rather than a file rewritten on every commit: a commit is one small
// append, and it inherits the log's fsync, checksums and crash recovery, so a
// commit torn by a crash is cut off on restart. The log is not compacted, so it
// grows with every commit and replay takes longer over time.
//
// The log lives outside data/topics/, so it is never listed or reachable as a topic.

import type { LogRecord } from "../common/types.js";
import { BinaryReader, BinaryWriter } from "../protocol/binary.js";
import { FileLog, type FileLogOptions } from "../storage/file-log.js";
import { LogError } from "../storage/log.js";
import type { TailTruncation } from "../storage/segment.js";

// The first byte of every value, so a future record layout can be told apart.
const RECORD_FORMAT_VERSION = 0;

const REPLAY_CHUNK = 1000;

/** The committed offsets of consumer groups, kept in their own log on disk. */
export class OffsetStore {
  /** What crash recovery cut off the end of the offsets log when it opened, or null. */
  readonly recoveredTail: TailTruncation | null;
  private readonly log: FileLog;
  private readonly offsets: Map<string, number>;
  private closed = false;

  // Private: opening needs `await`, so callers use OffsetStore.open().
  private constructor(log: FileLog, offsets: Map<string, number>) {
    this.log = log;
    this.offsets = offsets;
    this.recoveredTail = log.recoveredTail;
  }

  /**
   * Opens (or creates) the offsets log in `dir` and replays it. Rejects with
   * CORRUPT_LOG if a record in it can't be read as a commit.
   */
  static async open(dir: string, options: FileLogOptions = {}): Promise<OffsetStore> {
    // Crash recovery runs inside FileLog.open, so a torn commit is gone before replay.
    const log = await FileLog.open(dir, options);
    try {
      const offsets = await replay(log);
      return new OffsetStore(log, offsets);
    } catch (error) {
      await log.close();
      throw error;
    }
  }

  /** How many (group, topic, partition) positions have a committed offset. */
  get size(): number {
    return this.offsets.size;
  }

  /** The committed offset for this group and partition, or null if none was ever committed. */
  get(group: string, topic: string, partition: number): number | null {
    this.ensureOpen();
    return this.offsets.get(mapKey(group, topic, partition)) ?? null;
  }

  /**
   * Records `offset` as the committed offset for this group and partition. It
   * resolves only once the commit is durable on disk. Throws RangeError for
   * empty names or a negative or non-integer partition or offset.
   */
  // Moving an offset backwards is allowed: a group may deliberately re-process records.
  async commit(group: string, topic: string, partition: number, offset: number): Promise<void> {
    this.ensureOpen();
    // Callers (the group coordinator) validate client input first, so these are bugs.
    if (group.length === 0 || topic.length === 0) {
      throw new RangeError("group and topic must not be empty");
    }
    if (!Number.isSafeInteger(partition) || partition < 0) {
      throw new RangeError(`partition must be a non-negative integer, got ${partition}`);
    }
    if (!Number.isSafeInteger(offset) || offset < 0) {
      throw new RangeError(`offset must be a non-negative integer, got ${offset}`);
    }

    const record = { key: encodeKey(group, topic, partition), value: encodeValue(offset) };
    await this.log.append([record]);
    // Updated only after the commit is durable, so get() never reports one a crash could lose.
    this.offsets.set(mapKey(group, topic, partition), offset);
  }

  /** Closes the offsets log. Safe to call more than once. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.log.close();
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new LogError("LOG_CLOSED", "offset store is closed");
    }
  }
}

// A later record for a key overwrites an earlier one, leaving the latest commit.
async function replay(log: FileLog): Promise<Map<string, number>> {
  const offsets = new Map<string, number>();
  let next = 0;
  while (next < log.logEndOffset) {
    const records = await log.read(next, REPLAY_CHUNK);
    if (records.length === 0) {
      break;
    }
    for (const record of records) {
      const commit = decodeRecord(record);
      offsets.set(commit.mapKey, commit.offset);
    }
    next = records.at(-1)!.offset + 1;
  }
  return offsets;
}

// Record format:
//   key:    group string | topic string | partition u32
//   value:  format version u8 | committed offset u64

function encodeKey(group: string, topic: string, partition: number): Buffer {
  const writer = new BinaryWriter();
  writer.string(group);
  writer.string(topic);
  writer.u32(partition);
  return writer.toBuffer();
}

function encodeValue(offset: number): Buffer {
  const writer = new BinaryWriter();
  writer.u8(RECORD_FORMAT_VERSION);
  writer.u64(offset);
  return writer.toBuffer();
}

// The log's checksums rule out disk damage, so a record that doesn't fit the format
// was written by something else. Starting with wrong offsets would be worse than
// not starting, so it is refused.
function decodeRecord(record: LogRecord): { mapKey: string; offset: number } {
  if (record.key === null) {
    throw corrupt(record.offset, "it has no key");
  }
  try {
    const key = new BinaryReader(record.key);
    const group = key.string();
    const topic = key.string();
    const partition = key.u32();
    key.end();

    const value = new BinaryReader(record.value);
    const version = value.u8();
    if (version !== RECORD_FORMAT_VERSION) {
      throw corrupt(record.offset, `it has unknown format version ${version}`);
    }
    const offset = value.u64();
    value.end();

    return { mapKey: mapKey(group, topic, partition), offset };
  } catch (error) {
    if (error instanceof LogError) {
      throw error;
    }
    throw corrupt(record.offset, (error as Error).message);
  }
}

// JSON of the parts, so two different positions can never share a key, whatever
// characters their names contain (joining with a separator could collide).
function mapKey(group: string, topic: string, partition: number): string {
  return JSON.stringify([group, topic, partition]);
}

function corrupt(logOffset: number, detail: string): LogError {
  return new LogError("CORRUPT_LOG", `offsets log record ${logOffset} can't be read: ${detail}`);
}
