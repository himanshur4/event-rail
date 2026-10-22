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
 * - Reading at logEndOffset returns [] (caught up); reading past it is an error.
 * - Callers cannot modify stored records through the buffers they pass in or get back.
 */
export interface Log {
  /** The offset the next appended record will receive. */
  readonly logEndOffset: number;
  /** Appends a non-empty batch and returns the offsets it was assigned. */
  append(records: readonly RecordInput[]): Promise<AppendResult>;
  /** Returns up to `maxRecords` records, starting at `offset`. */
  read(offset: number, maxRecords: number): Promise<LogRecord[]>;
}

/** Machine-readable failure reasons, so callers can branch without parsing messages. */
export type LogErrorCode =
  | "EMPTY_BATCH"
  | "INVALID_OFFSET"
  | "OFFSET_OUT_OF_RANGE"
  | "INVALID_MAX_RECORDS";

export class LogError extends Error {
  readonly code: LogErrorCode;

  constructor(code: LogErrorCode, message: string) {
    super(message);
    this.name = "LogError";
    this.code = code;
  }
}
