import type { RecordInput } from "../common/types.js";
import { Partitioner } from "../topic/partitioner.js";
import { BrokerError } from "./client-errors.js";
import type { Connection } from "./connection.js";

/** Where one sent record was stored. */
export interface RecordMetadata {
  readonly partition: number;
  readonly offset: number;
}

// The records going to one partition, and where each came from in the caller's list.
interface PartitionBatch {
  readonly records: RecordInput[];
  readonly indexes: number[];
}

/** Sends records to topics, choosing a partition for each record. */
export class Producer {
  private readonly connection: Connection;
  private readonly partitioner = new Partitioner();
  // Never goes out of date: partition counts are fixed when a topic is created.
  private readonly partitionCounts = new Map<string, number>();

  constructor(connection: Connection) {
    this.connection = connection;
  }

  /**
   * Stores `records` in `topic` and resolves with where each one went, in the
   * same order. Records with the same key always go to the same partition.
   * Rejects with BrokerError (e.g. UNKNOWN_TOPIC, RECORD_TOO_LARGE) or
   * ConnectionError. A send is not atomic across partitions: on rejection, some
   * partitions may have been written.
   */
  async send(topic: string, records: readonly RecordInput[]): Promise<RecordMetadata[]> {
    if (records.length === 0) {
      return [];
    }
    const partitionCount = await this.partitionCount(topic);

    const batches = new Map<number, PartitionBatch>();
    records.forEach((record, index) => {
      const partition = this.partitioner.partition(record.key, partitionCount);
      let batch = batches.get(partition);
      if (batch === undefined) {
        batch = { records: [], indexes: [] };
        batches.set(partition, batch);
      }
      batch.records.push(record);
      batch.indexes.push(index);
    });

    const results: RecordMetadata[] = new Array(records.length);
    const sends = [...batches].map(async ([partition, batch]) => {
      const response = await this.connection.send({ type: "produce", topic, partition, records: batch.records });
      // The log gives a batch consecutive offsets, in order.
      batch.indexes.forEach((recordIndex, position) => {
        results[recordIndex] = { partition, offset: response.baseOffset + position };
      });
    });
    // Waits for every request, so none is still running when send() returns or throws.
    const outcomes = await Promise.allSettled(sends);
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        throw outcome.reason;
      }
    }
    return results;
  }

  private async partitionCount(topic: string): Promise<number> {
    const known = this.partitionCounts.get(topic);
    if (known !== undefined) {
      return known;
    }
    const metadata = await this.connection.send({ type: "metadata" });
    for (const info of metadata.topics) {
      this.partitionCounts.set(info.name, info.partitionCount);
    }
    const count = this.partitionCounts.get(topic);
    if (count === undefined) {
      // The same code and message the broker itself uses for a missing topic.
      throw new BrokerError("UNKNOWN_TOPIC", `topic ${topic} does not exist`);
    }
    return count;
  }
}
