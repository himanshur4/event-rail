// Every request and response, and the header that starts each one. codec.ts
// defines their exact bytes.
//
//   apiKey  type          request fields                    response fields
//   0       createTopic   topic, partitionCount             (none)
//   1       metadata      (none)                            every topic and its partition count
//   2       produce       topic, partition, records         offsets given to the records
//   3       fetch         topic, partition, offset,         log end offset, records
//                         maxRecords
//   4       joinGroup     group, topic, memberId            memberId, status, generation, partitions
//   5       heartbeat     group, memberId, generation       (none)
//   6       leaveGroup    group, memberId                   (none)
//   7       commitOffset  group, memberId, generation,      (none)
//                         topic, partition, offset
//   8       fetchOffset   group, topic, partition           committed offset, or none
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

/** Joins a consumer group, or asks again while the group rebalances. */
export interface JoinGroupRequest {
  readonly type: "joinGroup";
  readonly group: string;
  readonly topic: string;
  /** "" for a new member; afterwards, the id the broker gave it. */
  readonly memberId: string;
}

/** Tells the broker the member is still alive and still in `generation`. */
export interface HeartbeatRequest {
  readonly type: "heartbeat";
  readonly group: string;
  readonly memberId: string;
  readonly generation: number;
}

/** Leaves the group, so its partitions move to other members at once. */
export interface LeaveGroupRequest {
  readonly type: "leaveGroup";
  readonly group: string;
  readonly memberId: string;
}

/** Records the next offset the group will read from one of the member's partitions. */
export interface CommitOffsetRequest {
  readonly type: "commitOffset";
  readonly group: string;
  readonly memberId: string;
  readonly generation: number;
  readonly topic: string;
  readonly partition: number;
  readonly offset: number;
}

/** Asks for the group's committed offset for one partition. */
export interface FetchOffsetRequest {
  readonly type: "fetchOffset";
  readonly group: string;
  readonly topic: string;
  readonly partition: number;
}

/** Any request. Check `type` to see which one. */
export type ApiRequest =
  | CreateTopicRequest
  | MetadataRequest
  | ProduceRequest
  | FetchRequest
  | JoinGroupRequest
  | HeartbeatRequest
  | LeaveGroupRequest
  | CommitOffsetRequest
  | FetchOffsetRequest;

/** The name of an API, such as "produce" or "joinGroup". */
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

/** "waiting" while the group rebalances (ask again), then the member's assignment. */
export interface JoinGroupResponse {
  readonly type: "joinGroup";
  readonly memberId: string;
  readonly status: "waiting" | "assigned";
  /** 0 while waiting. */
  readonly generation: number;
  readonly partitions: readonly number[];
}

/** The member is alive and its generation is current. */
export interface HeartbeatResponse {
  readonly type: "heartbeat";
}

/** The member has left the group. */
export interface LeaveGroupResponse {
  readonly type: "leaveGroup";
}

/** The commit is durable. */
export interface CommitOffsetResponse {
  readonly type: "commitOffset";
}

/** The committed offset, or null if the group has none for that partition. */
export interface FetchOffsetResponse {
  readonly type: "fetchOffset";
  readonly offset: number | null;
}

/** The request failed: `code` says why for programs, `message` explains it for people. */
export interface ErrorResponse {
  readonly type: "error";
  readonly code: ErrorCode;
  readonly message: string;
}

/** The response to a request that succeeded. */
export type SuccessResponse =
  | CreateTopicResponse
  | MetadataResponse
  | ProduceResponse
  | FetchResponse
  | JoinGroupResponse
  | HeartbeatResponse
  | LeaveGroupResponse
  | CommitOffsetResponse
  | FetchOffsetResponse;

/** Any response: a success, or an error. */
export type ApiResponse = SuccessResponse | ErrorResponse;

/** The number that identifies each API on the wire. These never change once released. */
export const API_KEYS: Readonly<Record<ApiName, number>> = {
  createTopic: 0,
  metadata: 1,
  produce: 2,
  fetch: 3,
  joinGroup: 4,
  heartbeat: 5,
  leaveGroup: 6,
  commitOffset: 7,
  fetchOffset: 8,
};

/** The layout version of every API in this code. */
// When an API's layout changes it gets a new version and the broker learns to
// read both, so clients built for the old layout keep working.
export const API_VERSION = 0;
