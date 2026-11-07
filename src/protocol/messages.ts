// Every request and response, and the header that starts each one. codec.ts
// defines their exact bytes.
//
//   apiKey  type          request fields                    response fields
//   0       createTopic   topic, partitionCount             (none)
//   1       metadata      (none)                            every topic and its partition count
//   2       produce       topic, partition, records         offsets given to the records
//   3       fetch         topic, partition, offset,         log end offset, records
//                         maxRecords
//
// Any response can instead be an error: a code and a message.

import type { LogRecord, RecordInput } from "../common/types.js";
import type { ErrorCode } from "./error-codes.js";

/** The 8 bytes at the start of every request and every response. */
export interface MessageHeader {
  /** Which API the message belongs to (see API_KEYS). A u16 on the wire. */
  readonly apiKey: number;
  /** Which version of that API's layout the message uses. A u16 on the wire. */
  readonly apiVersion: number;
  /** Chosen by the client; a response repeats its request's value. A u32 on the wire. */
  readonly correlationId: number;
}

/** Creates a topic with `partitionCount` empty partitions. */
export interface CreateTopicRequest {
  readonly type: "createTopic";
  readonly topic: string;
  readonly partitionCount: number;
}

/** Asks for every topic and its partition count. */
export interface MetadataRequest {
  readonly type: "metadata";
}

/** Appends records to one partition of a topic. */
// The producer chooses the partition: with several brokers it must know the
// partition before it can tell which broker to send the records to.
export interface ProduceRequest {
  readonly type: "produce";
  readonly topic: string;
  readonly partition: number;
  readonly records: readonly RecordInput[];
}

/** Reads up to `maxRecords` records from one partition, starting at `offset`. */
export interface FetchRequest {
  readonly type: "fetch";
  readonly topic: string;
  readonly partition: number;
  readonly offset: number;
  readonly maxRecords: number;
}

/** Any request. Check `type` to see which one. */
export type ApiRequest = CreateTopicRequest | MetadataRequest | ProduceRequest | FetchRequest;

/** The name of an API: "createTopic" | "metadata" | "produce" | "fetch". */
export type ApiName = ApiRequest["type"];

/** The topic was created. */
export interface CreateTopicResponse {
  readonly type: "createTopic";
}

/** One topic, as listed in a metadata response. */
export interface TopicInfo {
  readonly name: string;
  readonly partitionCount: number;
}

/** Every topic on the broker, sorted by name. */
export interface MetadataResponse {
  readonly type: "metadata";
  readonly topics: readonly TopicInfo[];
}

/** The offsets the log gave the produced records (both inclusive). */
export interface ProduceResponse {
  readonly type: "produce";
  readonly baseOffset: number;
  readonly lastOffset: number;
}

/** Records read from a partition, and where that partition's log ends. */
export interface FetchResponse {
  readonly type: "fetch";
  readonly logEndOffset: number;
  readonly records: readonly LogRecord[];
}

/** The request failed: `code` says why for programs, `message` explains it for people. */
export interface ErrorResponse {
  readonly type: "error";
  readonly code: ErrorCode;
  readonly message: string;
}

/** The response to a request that succeeded. */
export type SuccessResponse = CreateTopicResponse | MetadataResponse | ProduceResponse | FetchResponse;

/** Any response: a success, or an error. */
export type ApiResponse = SuccessResponse | ErrorResponse;

/** The number that identifies each API on the wire. These never change once released. */
export const API_KEYS: Readonly<Record<ApiName, number>> = {
  createTopic: 0,
  metadata: 1,
  produce: 2,
  fetch: 3,
};

/** The layout version of every API in this code. */
// When an API's layout changes it gets a new version and the broker learns to
// read both, so clients built for the old layout keep working.
export const API_VERSION = 0;
