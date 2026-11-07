import type { LogRecord } from "../common/types.js";
import type { Connection } from "./connection.js";

export interface ConsumerOptions {
  /** The offset to start reading from (default 0: the beginning). */
  readonly fromOffset?: number;
  /** Most records per poll (default 500). */
  readonly maxRecords?: number;
}

/**
 * Reads one partition of a topic, in offset order. The log keeps records after
 * they're read, so each consumer tracks its own position. The position lives only
 * in memory, so a restarted program starts again from `fromOffset`.
 */
export class Consumer {
  private readonly connection: Connection;
  readonly topic: string;
  readonly partition: number;
  private readonly maxRecords: number;
  private nextOffset: number;
  private lastLogEndOffset: number | null = null;

  constructor(connection: Connection, topic: string, partition: number, options: ConsumerOptions = {}) {
    this.connection = connection;
    this.topic = topic;
    this.partition = partition;
    this.nextOffset = options.fromOffset ?? 0;
    this.maxRecords = options.maxRecords ?? 500;
  }

  /** The offset of the next record poll() will return. */
  get position(): number {
    return this.nextOffset;
  }

  /** Records not yet read, as of the last poll (null before the first poll). */
  get lag(): number | null {
    return this.lastLogEndOffset === null ? null : this.lastLogEndOffset - this.nextOffset;
  }

  /** Makes the next poll() start at `offset`, for example to re-read records. */
  seek(offset: number): void {
    this.nextOffset = offset;
  }

  /**
   * Fetches the next records, at most `maxRecords`, and moves the position
   * past them. Resolves with [] when caught up. Rejects with BrokerError
   * (e.g. OFFSET_OUT_OF_RANGE after seeking past the end) or ConnectionError.
   */
  async poll(): Promise<readonly LogRecord[]> {
    const response = await this.connection.send({
      type: "fetch",
      topic: this.topic,
      partition: this.partition,
      offset: this.nextOffset,
      maxRecords: this.maxRecords,
    });
    const last = response.records.at(-1);
    if (last !== undefined) {
      this.nextOffset = last.offset + 1;
    }
    this.lastLogEndOffset = response.logEndOffset;
    return response.records;
  }
}
