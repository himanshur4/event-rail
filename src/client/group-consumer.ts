// A consumer that shares a topic's partitions with the other members of its group
// and continues from the group's committed offsets.
//
// poll() joins the group if needed, then fetches from every partition this member
// owns. A heartbeat runs in the background; when the broker answers that the group
// is rebalancing (or that this member is out of date), the next poll() commits the
// current positions, joins again, and reads from the new assignment. Committing
// after processing gives at-least-once delivery: records processed after the last
// commit are read again by whoever owns the partition next.

import type { LogRecord } from "../common/types.js";
import { BrokerError, ConnectionError } from "./client-errors.js";
import type { Connection } from "./connection.js";

export interface GroupConsumerOptions {
  /** How often to send a heartbeat (default 1 s). Must be well below the broker's session timeout. */
  readonly heartbeatIntervalMs?: number;
  /** How long to wait before asking again while the group rebalances (default 100 ms). */
  readonly joinRetryMs?: number;
  /** Most records per partition per poll (default 500). */
  readonly maxRecords?: number;
}

/** A record, with the partition it came from. */
export interface GroupRecord extends LogRecord {
  readonly partition: number;
}

// Answers that mean "join again" rather than "fail".
const REJOIN_CODES: ReadonlySet<string> = new Set([
  "REBALANCE_IN_PROGRESS",
  "STALE_GENERATION",
  "UNKNOWN_MEMBER",
  "NOT_PARTITION_OWNER",
]);

/** Reads a topic as one member of a consumer group. */
export class GroupConsumer {
  readonly group: string;
  readonly topic: string;
  private readonly connection: Connection;
  private readonly heartbeatIntervalMs: number;
  private readonly joinRetryMs: number;
  private readonly maxRecords: number;
  private memberIdValue = "";
  private generationValue = 0;
  private partitionsValue: readonly number[] = [];
  // Partition → the next offset to read from it.
  private readonly positions = new Map<number, number>();
  private needsJoin = true;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  // A connection failure seen by a heartbeat, reported by the next poll().
  private failure: Error | null = null;
  private closed = false;

  constructor(connection: Connection, group: string, topic: string, options: GroupConsumerOptions = {}) {
    this.connection = connection;
    this.group = group;
    this.topic = topic;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? 1000;
    this.joinRetryMs = options.joinRetryMs ?? 100;
    this.maxRecords = options.maxRecords ?? 500;
  }

  /** The id the broker gave this member, or "" before it has joined. */
  get memberId(): string {
    return this.memberIdValue;
  }

  /** The generation of the current assignment; 0 before the first join. */
  get generation(): number {
    return this.generationValue;
  }

  /** The partitions this member currently reads. */
  get partitions(): readonly number[] {
    return this.partitionsValue;
  }

  /**
   * Joins the group if needed, then returns the next records from every owned
   * partition and moves past them. Resolves with [] when caught up.
   */
  async poll(): Promise<GroupRecord[]> {
    this.ensureUsable();
    if (this.needsJoin) {
      await this.rejoin();
    }
    const records: GroupRecord[] = [];
    for (const partition of this.partitionsValue) {
      const offset = this.positions.get(partition) ?? 0;
      const response = await this.connection.send({ type: "fetch", topic: this.topic, partition, offset, maxRecords: this.maxRecords });
      for (const record of response.records) {
        records.push({ ...record, partition });
      }
      const last = response.records.at(-1);
      if (last !== undefined) {
        this.positions.set(partition, last.offset + 1);
      }
    }
    return records;
  }

  /**
   * Commits the current position of every owned partition. Resolves false if the
   * broker refused because the group moved on; the member then joins again on
   * the next poll(), and the partition's next owner re-reads those records.
   */
  async commit(): Promise<boolean> {
    this.ensureUsable();
    if (this.generationValue === 0) {
      return false;
    }
    const results = await Promise.all(
      this.partitionsValue.map(async (partition) => {
        try {
          await this.connection.send({
            type: "commitOffset",
            group: this.group,
            memberId: this.memberIdValue,
            generation: this.generationValue,
            topic: this.topic,
            partition,
            offset: this.positions.get(partition) ?? 0,
          });
          return true;
        } catch (error) {
          if (!isRejoinError(error)) {
            throw error;
          }
          this.needsJoin = true;
          return false;
        }
      }),
    );
    return results.every((committed) => committed);
  }

  /** Commits, leaves the group so its partitions move at once, and stops heartbeats. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.stopHeartbeats();
    if (this.memberIdValue !== "" && this.failure === null) {
      try {
        if (!this.needsJoin) {
          await this.commit();
        }
        await this.connection.send({ type: "leaveGroup", group: this.group, memberId: this.memberIdValue });
      } catch {
        // The broker removes a silent member after its session timeout anyway.
      }
    }
    this.closed = true;
  }

  // Saves progress while the old assignment still stands, then asks to join until
  // the group has a new assignment, and starts each partition at its committed offset.
  private async rejoin(): Promise<void> {
    this.stopHeartbeats();
    if (this.generationValue > 0) {
      await this.commit();
    }
    for (;;) {
      let result;
      try {
        result = await this.connection.send({ type: "joinGroup", group: this.group, topic: this.topic, memberId: this.memberIdValue });
      } catch (error) {
        if (error instanceof BrokerError && error.code === "UNKNOWN_MEMBER") {
          this.memberIdValue = ""; // removed: join as a new member
          continue;
        }
        throw error;
      }
      this.memberIdValue = result.memberId;
      if (result.status === "assigned") {
        this.generationValue = result.generation;
        this.partitionsValue = result.partitions;
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, this.joinRetryMs));
      this.ensureUsable();
    }

    this.positions.clear();
    for (const partition of this.partitionsValue) {
      const { offset } = await this.connection.send({ type: "fetchOffset", group: this.group, topic: this.topic, partition });
      this.positions.set(partition, offset ?? 0);
    }
    this.needsJoin = false;
    this.startHeartbeats();
  }

  private startHeartbeats(): void {
    this.heartbeatTimer = setInterval(() => void this.heartbeat(), this.heartbeatIntervalMs);
    // A consumer the program forgot to close doesn't keep the process alive.
    this.heartbeatTimer.unref();
  }

  private stopHeartbeats(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private async heartbeat(): Promise<void> {
    if (this.needsJoin || this.closed) {
      return;
    }
    const generation = this.generationValue;
    try {
      await this.connection.send({ type: "heartbeat", group: this.group, memberId: this.memberIdValue, generation });
    } catch (error) {
      // Only act on an answer about the assignment this heartbeat was sent for.
      if (generation !== this.generationValue) {
        return;
      }
      if (isRejoinError(error)) {
        this.needsJoin = true;
      } else {
        this.failure ??= error as Error;
      }
    }
  }

  private ensureUsable(): void {
    if (this.closed) {
      throw new ConnectionError("CONNECTION_CLOSED", "close() was called on this group consumer");
    }
    if (this.failure !== null) {
      throw this.failure;
    }
  }
}

function isRejoinError(error: unknown): boolean {
  return error instanceof BrokerError && REJOIN_CODES.has(error.code);
}
