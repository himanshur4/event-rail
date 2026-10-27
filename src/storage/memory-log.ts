import type { LogRecord, RecordInput } from "../common/types.js";
import { LogError, validateReadArguments, type AppendResult, type Log } from "./log.js";

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
  private closed = false;

  constructor(options: MemoryLogOptions = {}) {
    this.clock = options.clock ?? (() => Date.now());
  }

  get logEndOffset(): number {
    return this.records.length;
  }

  async append(records: readonly RecordInput[]): Promise<AppendResult> {
    this.ensureOpen();
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
    this.ensureOpen();
    validateReadArguments(offset, maxRecords, this.logEndOffset);

    // Return copies so callers cannot mutate stored records.
    return this.records.slice(offset, offset + maxRecords).map((record) => ({
      ...record,
      key: copyBuffer(record.key),
      value: Buffer.from(record.value),
    }));
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new LogError("LOG_CLOSED", "log is closed");
    }
  }
}

function copyBuffer(buffer: Buffer | null): Buffer | null {
  return buffer === null ? null : Buffer.from(buffer);
}
