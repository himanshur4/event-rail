import type { LogRecord, RecordInput } from "../common/types.js";

/** Offsets assigned to an appended batch (both inclusive). */
export interface AppendResult {
  readonly baseOffset: number;
  readonly lastOffset: number;
}

/**
 * An append-only sequence of records for one partition.
 *
 * Contract (verified by test/storage/log-contract.ts):
 * - Offsets start at 0 and increase by 1 with no gaps.
 * - A batch is appended entirely or not at all.
 * - Concurrent appends are applied in the order they were called.
 * - Reading at logEndOffset returns [] (caught up); reading past it is an error.
 * - Callers cannot modify stored records through the buffers they pass in or get back.
 * - After close(), append and read reject with LOG_CLOSED.
 */
export interface Log {
  /** The offset the next appended record will receive. */
  readonly logEndOffset: number;
  /** Appends a non-empty batch and returns the offsets it was assigned. */
  append(records: readonly RecordInput[]): Promise<AppendResult>;
  /** Returns up to `maxRecords` records, starting at `offset`. */
  read(offset: number, maxRecords: number): Promise<LogRecord[]>;
  /** Releases any resources held by the log. Safe to call more than once. */
  close(): Promise<void>;
}

/** Machine-readable failure reasons, so callers can branch without parsing messages. */
export type LogErrorCode =
  | "EMPTY_BATCH"
  | "INVALID_OFFSET"
  | "OFFSET_OUT_OF_RANGE"
  | "INVALID_MAX_RECORDS"
  | "LOG_CLOSED"
  | "CORRUPT_LOG";

export class LogError extends Error {
  readonly code: LogErrorCode;

  constructor(code: LogErrorCode, message: string) {
    super(message);
    this.name = "LogError";
    this.code = code;
  }
}

/** Throws the contract's LogError for invalid read arguments; shared so every implementation agrees. */
export function validateReadArguments(offset: number, maxRecords: number, logEndOffset: number): void {
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
  if (offset > logEndOffset) {
    throw new LogError(
      "OFFSET_OUT_OF_RANGE",
      `offset ${offset} is past the log end offset ${logEndOffset}`,
    );
  }
}
