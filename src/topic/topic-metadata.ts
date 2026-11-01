// A topic's definition, the rules for its name, and its topic.json file:
//
//   <dataDir>/topics/orders/topic.json   {"version":1,"name":"orders","partitionCount":3}
//   <dataDir>/topics/orders/0/, 1/, 2/   one FileLog directory per partition
//
// topic.json is the commit point of creating a topic: it is written last, and
// atomically, so a directory without it is a creation that never finished.

import { open, readFile, rename } from "node:fs/promises";
import { join } from "node:path";

/** Machine-readable failure reasons, so callers can branch without parsing messages. */
export type TopicErrorCode =
  | "INVALID_TOPIC_NAME"
  | "INVALID_PARTITION_COUNT"
  | "TOPIC_EXISTS"
  | "UNKNOWN_TOPIC"
  | "UNKNOWN_PARTITION"
  | "CORRUPT_TOPIC"
  | "MANAGER_CLOSED";

export class TopicError extends Error {
  readonly code: TopicErrorCode;

  constructor(code: TopicErrorCode, message: string) {
    super(message);
    this.name = "TopicError";
    this.code = code;
  }
}

/** Longest allowed topic name, so file paths stay short on every operating system. */
export const MAX_TOPIC_NAME_LENGTH = 100;

/** Most partitions a topic may have. Each one is a directory with open files. */
export const MAX_PARTITIONS = 1000;

/** The topic.json format version, so a future format can recognise older files. */
export const TOPIC_METADATA_VERSION = 1;

// A lowercase letter or digit, then lowercase letters, digits, '.', '_' or '-'.
// This rules out "." and "..", and any path separator.
const TOPIC_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]*$/;

// Device names Windows refuses as file or folder names, with or without an extension.
const WINDOWS_RESERVED_NAMES = new Set([
  "con",
  "prn",
  "aux",
  "nul",
  "com0",
  "com1",
  "com2",
  "com3",
  "com4",
  "com5",
  "com6",
  "com7",
  "com8",
  "com9",
  "lpt0",
  "lpt1",
  "lpt2",
  "lpt3",
  "lpt4",
  "lpt5",
  "lpt6",
  "lpt7",
  "lpt8",
  "lpt9",
]);

/**
 * Why `name` is not a valid topic name, or null if it is valid.
 * Names become directory names, so they must be safe on every operating system.
 */
export function topicNameProblem(name: string): string | null {
  if (name.length === 0) {
    return "must not be empty";
  }
  if (name.length > MAX_TOPIC_NAME_LENGTH) {
    return `must be at most ${MAX_TOPIC_NAME_LENGTH} characters`;
  }
  // Lowercase only: case-insensitive file systems (Windows, macOS) would map
  // "Orders" and "orders" to the same directory.
  if (!TOPIC_NAME_PATTERN.test(name)) {
    return "may only contain lowercase letters, digits, '.', '_' and '-', and must start with a letter or digit";
  }
  // Windows drops trailing dots, so "orders." would collide with "orders".
  if (name.endsWith(".")) {
    return "must not end with '.'";
  }
  if (WINDOWS_RESERVED_NAMES.has(name.split(".")[0] ?? name)) {
    return "is a reserved device name on Windows";
  }
  return null;
}

/** Throws INVALID_TOPIC_NAME if `name` breaks the naming rules. */
export function validateTopicName(name: string): void {
  const problem = topicNameProblem(name);
  if (problem !== null) {
    throw new TopicError("INVALID_TOPIC_NAME", `topic name ${JSON.stringify(name)} ${problem}`);
  }
}

/** Throws INVALID_PARTITION_COUNT unless 1 <= `partitionCount` <= MAX_PARTITIONS. */
export function validatePartitionCount(partitionCount: number): void {
  if (!isValidPartitionCount(partitionCount)) {
    throw new TopicError(
      "INVALID_PARTITION_COUNT",
      `partition count must be a whole number from 1 to ${MAX_PARTITIONS}, got ${partitionCount}`,
    );
  }
}

function isValidPartitionCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 1 && value <= MAX_PARTITIONS;
}

/** A topic's definition, as stored in its topic.json file. */
export interface TopicMetadata {
  readonly version: typeof TOPIC_METADATA_VERSION;
  readonly name: string;
  readonly partitionCount: number;
}

const METADATA_FILE = "topic.json";

/**
 * Writes topic.json into `topicDir` atomically: a crash leaves either no
 * topic.json or the complete one, never a partial file.
 */
// Written to a temporary file and fsynced, then renamed into place. A rename
// is a single step, so readers see either no file or the complete one. The
// directory itself is not fsynced.
export async function writeTopicMetadata(topicDir: string, metadata: TopicMetadata): Promise<void> {
  const finalPath = join(topicDir, METADATA_FILE);
  const tempPath = `${finalPath}.tmp`;
  // Built field by field so the key order is always the same.
  const contents = `${JSON.stringify({
    version: metadata.version,
    name: metadata.name,
    partitionCount: metadata.partitionCount,
  })}\n`;

  const file = await open(tempPath, "w");
  try {
    await file.writeFile(contents);
    await file.sync();
  } finally {
    await file.close();
  }
  await rename(tempPath, finalPath);
}

/**
 * Reads the topic.json in `topicDir`. Returns null if there is none (the
 * topic's creation never completed). Throws CORRUPT_TOPIC if the file exists
 * but is invalid or describes a different topic.
 */
export async function readTopicMetadata(topicDir: string, expectedName: string): Promise<TopicMetadata | null> {
  let text: string;
  try {
    text = await readFile(join(topicDir, METADATA_FILE), "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return null;
    }
    throw error;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw corrupt(expectedName, "topic.json is not valid JSON");
  }
  if (typeof parsed !== "object" || parsed === null) {
    throw corrupt(expectedName, "topic.json does not contain an object");
  }

  // Each field is checked below before it is trusted.
  const { version, name, partitionCount } = parsed as Record<string, unknown>;
  if (version !== TOPIC_METADATA_VERSION) {
    throw corrupt(expectedName, `topic.json has unsupported version ${String(version)}`);
  }
  // A mismatch means the directory was renamed or the file copied from elsewhere.
  if (name !== expectedName) {
    throw corrupt(expectedName, `topic.json names topic ${JSON.stringify(name)}`);
  }
  if (!isValidPartitionCount(partitionCount)) {
    throw corrupt(expectedName, `topic.json has invalid partition count ${String(partitionCount)}`);
  }
  return { version: TOPIC_METADATA_VERSION, name: expectedName, partitionCount };
}

function corrupt(topic: string, detail: string): TopicError {
  return new TopicError("CORRUPT_TOPIC", `topic ${topic}: ${detail}`);
}
