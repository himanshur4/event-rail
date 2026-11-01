// Owns every topic on this broker: creates topics, opens them at startup, and
// returns the Log for a given (topic, partition). Each partition is an
// independent FileLog with its own offsets, append queue, and crash recovery,
// so appends to different partitions never wait for each other.
//
// Current limits:
// - A topic's partition count can't change after creation, since keys would
//   map to different partitions.
// - Topics can't be deleted yet.
// - Partitions are opened one after another at startup.
// - Only one process may use a data directory at a time; this is not enforced.

import { mkdir, readdir, rm, stat } from "node:fs/promises";
import { join } from "node:path";

import { FileLog, type FileLogOptions } from "../storage/file-log.js";
import type { Log } from "../storage/log.js";
import type { TailTruncation } from "../storage/segment.js";
import {
  TOPIC_METADATA_VERSION,
  TopicError,
  readTopicMetadata,
  topicNameProblem,
  validatePartitionCount,
  validateTopicName,
  writeTopicMetadata,
  type TopicMetadata,
} from "./topic-metadata.js";

export interface TopicManagerOptions {
  /** Options passed to every partition's FileLog (fsync policy, segment size, clock). */
  readonly logOptions?: FileLogOptions;
}

/** A torn write that crash recovery cut off one partition's log at startup. */
export interface PartitionRecovery {
  readonly topic: string;
  readonly partition: number;
  readonly tail: TailTruncation;
}

interface OpenTopic {
  readonly metadata: TopicMetadata;
  readonly partitions: readonly FileLog[];
}

/** Creates, opens, and closes topics, and gives access to each partition's Log. */
export class TopicManager {
  private readonly topicsDir: string;
  private readonly logOptions: FileLogOptions;
  private readonly topics = new Map<string, OpenTopic>();
  // Creations in progress, so a duplicate name is rejected and close() can wait for them.
  private readonly creating = new Map<string, Promise<TopicMetadata>>();
  private readonly recoveries: PartitionRecovery[] = [];
  private closed = false;

  // Private: opening needs `await`, so callers use TopicManager.open().
  private constructor(topicsDir: string, logOptions: FileLogOptions) {
    this.topicsDir = topicsDir;
    this.logOptions = logOptions;
  }

  /**
   * Opens every topic stored under `dataDir`, recovering each partition's log.
   * Rejects with CORRUPT_TOPIC if a topic's topic.json is invalid or one of its
   * partition directories is missing.
   */
  static async open(dataDir: string, options: TopicManagerOptions = {}): Promise<TopicManager> {
    const topicsDir = join(dataDir, "topics");
    await mkdir(topicsDir, { recursive: true });
    const manager = new TopicManager(topicsDir, options.logOptions ?? {});

    try {
      const entries = await readdir(topicsDir, { withFileTypes: true });
      // Sorted so topics always open in the same order.
      const names = entries
        .filter((entry) => entry.isDirectory() && topicNameProblem(entry.name) === null)
        .map((entry) => entry.name)
        .sort();

      for (const name of names) {
        const metadata = await readTopicMetadata(join(topicsDir, name), name);
        // No topic.json: a creation that crashed before its commit point. It
        // never held acknowledged data, so it is not a topic.
        if (metadata === null) {
          continue;
        }
        const partitions = await manager.openPartitions(metadata, true);
        manager.topics.set(name, { metadata, partitions });
      }
      return manager;
    } catch (error) {
      await manager.close();
      throw error;
    }
  }

  /** Torn writes that crash recovery cut off partition logs when this manager opened. */
  get recoveredTails(): readonly PartitionRecovery[] {
    return this.recoveries;
  }

  /**
   * Creates a topic with `partitionCount` empty partitions. Rejects with
   * INVALID_TOPIC_NAME, INVALID_PARTITION_COUNT, or TOPIC_EXISTS.
   */
  async createTopic(name: string, partitionCount: number): Promise<TopicMetadata> {
    this.ensureOpen();
    validateTopicName(name);
    validatePartitionCount(partitionCount);

    // There is no await between this check and registering the creation, so a
    // concurrent create of the same name is guaranteed to see it.
    if (this.topics.has(name) || this.creating.has(name)) {
      throw new TopicError("TOPIC_EXISTS", `topic ${name} already exists`);
    }
    const creation = this.createNow(name, partitionCount);
    this.creating.set(name, creation);
    try {
      return await creation;
    } finally {
      this.creating.delete(name);
    }
  }

  /** Every topic, sorted by name. */
  listTopics(): TopicMetadata[] {
    this.ensureOpen();
    return [...this.topics.values()].map((topic) => topic.metadata).sort(compareByName);
  }

  /**
   * The Log of one partition of a topic. Throws UNKNOWN_TOPIC or UNKNOWN_PARTITION.
   */
  partition(topic: string, partition: number): Log {
    this.ensureOpen();
    const entry = this.topics.get(topic);
    if (entry === undefined) {
      throw new TopicError("UNKNOWN_TOPIC", `topic ${topic} does not exist`);
    }
    const log = Number.isSafeInteger(partition) ? entry.partitions[partition] : undefined;
    if (log === undefined) {
      throw new TopicError(
        "UNKNOWN_PARTITION",
        `topic ${topic} has no partition ${partition} (it has ${entry.metadata.partitionCount})`,
      );
    }
    return log;
  }

  /** Closes every partition's log. Safe to call more than once. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    // Creations already in progress finish first, so their partitions are closed too.
    await Promise.allSettled(this.creating.values());
    const logs = [...this.topics.values()].flatMap((topic) => topic.partitions);
    await Promise.all(logs.map((log) => log.close()));
  }

  // The order of steps makes a crash at any point harmless: leftovers of an
  // earlier unfinished creation are removed, the partitions are created, and
  // topic.json is written last as the commit point.
  private async createNow(name: string, partitionCount: number): Promise<TopicMetadata> {
    const topicDir = join(this.topicsDir, name);
    // A directory without topic.json never held acknowledged data (appends are
    // only possible after the commit point), so it is safe to delete.
    await rm(topicDir, { recursive: true, force: true });
    await mkdir(topicDir);

    const metadata: TopicMetadata = { version: TOPIC_METADATA_VERSION, name, partitionCount };
    const partitions = await this.openPartitions(metadata, false);
    try {
      await writeTopicMetadata(topicDir, metadata);
    } catch (error) {
      await Promise.all(partitions.map((log) => log.close()));
      throw error;
    }
    this.topics.set(name, { metadata, partitions });
    return metadata;
  }

  // With `mustExist`, a missing partition directory is damage: FileLog.open
  // would otherwise create an empty log there and silently lose its data.
  private async openPartitions(metadata: TopicMetadata, mustExist: boolean): Promise<FileLog[]> {
    const partitions: FileLog[] = [];
    try {
      for (let partition = 0; partition < metadata.partitionCount; partition++) {
        const dir = join(this.topicsDir, metadata.name, String(partition));
        if (mustExist && !(await isDirectory(dir))) {
          throw new TopicError(
            "CORRUPT_TOPIC",
            `topic ${metadata.name}: partition ${partition} directory is missing`,
          );
        }
        const log = await FileLog.open(dir, this.logOptions);
        partitions.push(log);
        if (log.recoveredTail !== null) {
          this.recoveries.push({ topic: metadata.name, partition, tail: log.recoveredTail });
        }
      }
      return partitions;
    } catch (error) {
      await Promise.all(partitions.map((log) => log.close()));
      throw error;
    }
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new TopicError("MANAGER_CLOSED", "topic manager is closed");
    }
  }
}

function compareByName(a: TopicMetadata, b: TopicMetadata): number {
  if (a.name < b.name) {
    return -1;
  }
  return a.name > b.name ? 1 : 0;
}

// False if `path` doesn't exist.
async function isDirectory(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isDirectory();
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}
