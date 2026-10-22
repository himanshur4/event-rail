/** A record as supplied by a producer. The log, not the producer, assigns its offset. */
export interface RecordInput {
  /** Optional key, intended for partition routing. */
  readonly key: Buffer | null;
  readonly value: Buffer;
}

/** A record as stored in a log. */
export interface LogRecord {
  /** Position in the log: zero-based, contiguous, never reused. */
  readonly offset: number;
  /** Append time in Unix epoch milliseconds, shared by all records in a batch. */
  readonly timestamp: number;
  readonly key: Buffer | null;
  readonly value: Buffer;
}
