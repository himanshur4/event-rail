import type { LogRecord, RecordInput } from "../common/types.js";
import { LogError, type AppendResult, type Log } from "./log.js";

export interface MemoryLogOptions {
  /** Returns the current time in milliseconds. Injectable so tests control timestamps. */
  readonly clock?: () => number;
}

/**
 * A Log backed by an in-memory array. Nothing survives a process restart; it
 * serves as the reference implementation of the Log contract.
 */
export class MemoryLog implements Log {
  // Invariant: records[i].offset === i, so looking up an offset is an array index.
  private readonly records: LogRecord[] = [];
  private readonly clock: () => number;

  constructor(options: MemoryLogOptions = {}) {
    this.clock = options.clock ?? (() => Date.now());
  }

  get logEndOffset(): number {
    return this.records.length;
  }

  async append(records: readonly RecordInput[]): Promise<AppendResult> {
    if (records.length === 0) {
      throw new LogError("EMPTY_BATCH", "cannot append an empty batch");
    }

    const baseOffset = this.logEndOffset;
    const timestamp = this.clock();

    // Build the whole batch before storing any of it, so a failure part-way
    // through never leaves a partial batch. Buffers are copied so later changes
    // to the caller's buffers cannot rewrite stored history.
    const batch: LogRecord[] = records.map((input, i) => ({
      offset: baseOffset + i,
      timestamp,
      key: copyBuffer(input.key),
      value: Buffer.from(input.value),
    }));

    // Pushed one at a time: spreading a very large batch into push() can exceed
    // the engine's argument limit.
    for (const record of batch) {
      this.records.push(record);
    }

    return { baseOffset, lastOffset: baseOffset + batch.length - 1 };
  }

  async read(offset: number, maxRecords: number): Promise<LogRecord[]> {
    if (!Number.isSafeInteger(offset) || offset < 0) {
      throw new LogError("INVALID_OFFSET", `offset must be a non-negative integer, got ${offset}`);
    }
    // A limit of 0 would always return [] and look like "caught up" to a consumer.
    if (!Number.isSafeInteger(maxRecords) || maxRecords < 1) {
      throw new LogError(
        "INVALID_MAX_RECORDS",
        `maxRecords must be a positive integer, got ${maxRecords}`,
      );
    }
    // Reading at the end is the normal caught-up state; past the end is an error.
    if (offset > this.logEndOffset) {
      throw new LogError(
        "OFFSET_OUT_OF_RANGE",
        `offset ${offset} is past the log end offset ${this.logEndOffset}`,
      );
    }

    // Return copies so callers cannot mutate stored records.
    return this.records.slice(offset, offset + maxRecords).map((record) => ({
      ...record,
      key: copyBuffer(record.key),
      value: Buffer.from(record.value),
    }));
  }
}

function copyBuffer(buffer: Buffer | null): Buffer | null {
  return buffer === null ? null : Buffer.from(buffer);
}
