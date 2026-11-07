// Turns the messages of messages.ts into frame payloads, and payloads back into
// messages.
//
// Every request and response starts with the same 8-byte header, whose layout
// never changes in any version, so a broker can always answer, even if only to
// say it doesn't support that version:
//
//   apiKey u16 | apiVersion u16 | correlationId u32
//
// A response starts with its request's header, then an error number (u16):
//   0 (NO_ERROR)   -> the API's response fields follow
//   anything else  -> an error message (string) follows, and nothing else
// So an error has the same layout for every API, and can even answer a request
// for an API the broker doesn't know.
//
// Example: createTopic "orders" with 3 partitions, correlation ID 7:
//
//   00 00                     apiKey 0 (createTopic)
//   00 00                     apiVersion 0
//   00 00 00 07               correlationId 7
//   00 06 6f 72 64 65 72 73   topic: a 6-byte string, "orders"
//   00 00 00 03               partitionCount 3
//
// The codec checks a message's shape, not its meaning: a partition count of 0
// decodes fine, and TopicManager rejects it. Each rule lives in one place.

import type { LogRecord, RecordInput } from "../common/types.js";
import { BinaryReader, BinaryWriter } from "./binary.js";
import { NO_ERROR, errorCodeFromWire, errorCodeToWire } from "./error-codes.js";
import {
  API_KEYS,
  API_VERSION,
  type ApiRequest,
  type ApiResponse,
  type CreateTopicRequest,
  type FetchRequest,
  type FetchResponse,
  type MessageHeader,
  type MetadataResponse,
  type ProduceRequest,
  type ProduceResponse,
  type SuccessResponse,
  type TopicInfo,
} from "./messages.js";
import { ProtocolError } from "./protocol-error.js";

/** Longest error message a response carries, in characters; longer ones are cut short. */
export const MAX_ERROR_MESSAGE_LENGTH = 1000;

/** Bytes in a fetch response before its first record: header, error number, log end offset, record count. */
export const FETCH_RESPONSE_BASE_BYTES = 8 + 2 + 8 + 4;

/** Bytes one record adds to a fetch response. */
// Lives here because only the codec knows the layout; the broker uses it to keep
// a fetch response within one frame.
export function fetchRecordBytes(record: LogRecord): number {
  const keyBytes = record.key === null ? 0 : record.key.length;
  return 8 + 8 + 4 + keyBytes + 4 + record.value.length;
}

/** Encodes a request as a frame payload: the header, then the request's fields. */
export function encodeRequest(correlationId: number, request: ApiRequest): Buffer {
  const writer = new BinaryWriter();
  writeHeader(writer, { apiKey: API_KEYS[request.type], apiVersion: API_VERSION, correlationId });
  writeRequestBody(writer, request);
  return writer.toBuffer();
}

/**
 * Decodes a request from a frame payload. Throws ProtocolError:
 * UNSUPPORTED_VERSION if this code can't read the header's API and version,
 * or MALFORMED_MESSAGE if the bytes don't fit that API's layout.
 */
export function decodeRequest(payload: Buffer): ApiRequest {
  const reader = new BinaryReader(payload);
  const header = readHeader(reader);
  const request = readRequestBody(header, reader);
  reader.end();
  return request;
}

/**
 * Encodes the response to the request whose header is `requestHeader`. The
 * response starts with that same header, so the client can match it to its
 * request. An error message over MAX_ERROR_MESSAGE_LENGTH characters is cut.
 */
export function encodeResponse(requestHeader: MessageHeader, response: ApiResponse): Buffer {
  const writer = new BinaryWriter();
  writeHeader(writer, requestHeader);
  if (response.type === "error") {
    writer.u16(errorCodeToWire(response.code));
    writer.string(shorten(response.message));
  } else {
    // A success response only makes sense as the answer to its own API, in the
    // version this code writes. Anything else is a bug in the broker.
    if (requestHeader.apiKey !== API_KEYS[response.type] || requestHeader.apiVersion !== API_VERSION) {
      throw new RangeError(
        `a ${response.type} response can't answer apiKey ${requestHeader.apiKey} version ${requestHeader.apiVersion}`,
      );
    }
    writer.u16(NO_ERROR);
    writeResponseBody(writer, response);
  }
  return writer.toBuffer();
}

/**
 * Decodes a response from a frame payload. An error response decodes to an
 * ErrorResponse; it is not thrown. Throws ProtocolError like decodeRequest.
 */
export function decodeResponse(payload: Buffer): ApiResponse {
  const reader = new BinaryReader(payload);
  const header = readHeader(reader);
  const errorNumber = reader.u16();
  let response: ApiResponse;
  if (errorNumber === NO_ERROR) {
    response = readResponseBody(header, reader);
  } else {
    const message = reader.string();
    response = { type: "error", code: errorCodeFromWire(errorNumber), message };
  }
  reader.end();
  return response;
}

/**
 * Reads only the header at the start of a request or response payload.
 * Throws MALFORMED_MESSAGE if the payload is shorter than a header.
 */
// Receivers call this first, so they know the correlation ID even if the rest
// of the message fails to decode.
export function decodeHeader(payload: Buffer): MessageHeader {
  return readHeader(new BinaryReader(payload));
}

function writeHeader(writer: BinaryWriter, header: MessageHeader): void {
  writer.u16(header.apiKey);
  writer.u16(header.apiVersion);
  writer.u32(header.correlationId);
}

function readHeader(reader: BinaryReader): MessageHeader {
  const apiKey = reader.u16();
  const apiVersion = reader.u16();
  const correlationId = reader.u32();
  return { apiKey, apiVersion, correlationId };
}

function writeRequestBody(writer: BinaryWriter, request: ApiRequest): void {
  switch (request.type) {
    case "createTopic":
      writeCreateTopicRequest(writer, request);
      break;
    case "metadata":
      break;
    case "produce":
      writeProduceRequest(writer, request);
      break;
    case "fetch":
      writeFetchRequest(writer, request);
      break;
    default:
      unreachable(request);
  }
}

function readRequestBody(header: MessageHeader, reader: BinaryReader): ApiRequest {
  if (header.apiVersion !== API_VERSION) {
    throw unsupported(header);
  }
  switch (header.apiKey) {
    case API_KEYS.createTopic:
      return readCreateTopicRequest(reader);
    case API_KEYS.metadata:
      return { type: "metadata" };
    case API_KEYS.produce:
      return readProduceRequest(reader);
    case API_KEYS.fetch:
      return readFetchRequest(reader);
    default:
      throw unsupported(header);
  }
}

function writeResponseBody(writer: BinaryWriter, response: SuccessResponse): void {
  switch (response.type) {
    case "createTopic":
      break;
    case "metadata":
      writeMetadataResponse(writer, response);
      break;
    case "produce":
      writeProduceResponse(writer, response);
      break;
    case "fetch":
      writeFetchResponse(writer, response);
      break;
    default:
      unreachable(response);
  }
}

function readResponseBody(header: MessageHeader, reader: BinaryReader): SuccessResponse {
  if (header.apiVersion !== API_VERSION) {
    throw unsupported(header);
  }
  switch (header.apiKey) {
    case API_KEYS.createTopic:
      return { type: "createTopic" };
    case API_KEYS.metadata:
      return readMetadataResponse(reader);
    case API_KEYS.produce:
      return readProduceResponse(reader);
    case API_KEYS.fetch:
      return readFetchResponse(reader);
    default:
      throw unsupported(header);
  }
}

// createTopic (apiKey 0)
//   request:   topic string | partitionCount u32
//   response:  (no fields)

function writeCreateTopicRequest(writer: BinaryWriter, request: CreateTopicRequest): void {
  writer.string(request.topic);
  writer.u32(request.partitionCount);
}

function readCreateTopicRequest(reader: BinaryReader): CreateTopicRequest {
  const topic = reader.string();
  const partitionCount = reader.u32();
  return { type: "createTopic", topic, partitionCount };
}

// metadata (apiKey 1)
//   request:   (no fields)
//   response:  topic count u32 | for each topic: name string | partitionCount u32

function writeMetadataResponse(writer: BinaryWriter, response: MetadataResponse): void {
  writer.u32(response.topics.length);
  for (const topic of response.topics) {
    writer.string(topic.name);
    writer.u32(topic.partitionCount);
  }
}

function readMetadataResponse(reader: BinaryReader): MetadataResponse {
  const count = reader.u32();
  const topics: TopicInfo[] = [];
  for (let i = 0; i < count; i++) {
    const name = reader.string();
    const partitionCount = reader.u32();
    topics.push({ name, partitionCount });
  }
  return { type: "metadata", topics };
}

// produce (apiKey 2)
//   request:   topic string | partition u32 | record count u32 |
//              for each record: key nullableBytes | value bytes
//   response:  baseOffset u64 | lastOffset u64

function writeProduceRequest(writer: BinaryWriter, request: ProduceRequest): void {
  writer.string(request.topic);
  writer.u32(request.partition);
  writer.u32(request.records.length);
  for (const record of request.records) {
    writer.nullableBytes(record.key);
    writer.bytes(record.value);
  }
}

function readProduceRequest(reader: BinaryReader): ProduceRequest {
  const topic = reader.string();
  const partition = reader.u32();
  const count = reader.u32();
  // `count` is untrusted, so no space is reserved for it. A lie can't make this
  // loop spin either: each record reads at least 8 bytes, so it fails with
  // MALFORMED_MESSAGE as soon as the payload runs out.
  const records: RecordInput[] = [];
  for (let i = 0; i < count; i++) {
    const key = reader.nullableBytes();
    const value = reader.bytes();
    records.push({ key, value });
  }
  return { type: "produce", topic, partition, records };
}

function writeProduceResponse(writer: BinaryWriter, response: ProduceResponse): void {
  writer.u64(response.baseOffset);
  writer.u64(response.lastOffset);
}

function readProduceResponse(reader: BinaryReader): ProduceResponse {
  const baseOffset = reader.u64();
  const lastOffset = reader.u64();
  return { type: "produce", baseOffset, lastOffset };
}

// fetch (apiKey 3)
//   request:   topic string | partition u32 | offset u64 | maxRecords u32
//   response:  logEndOffset u64 | record count u32 |
//              for each record: offset u64 | timestamp i64 | key nullableBytes | value bytes

function writeFetchRequest(writer: BinaryWriter, request: FetchRequest): void {
  writer.string(request.topic);
  writer.u32(request.partition);
  writer.u64(request.offset);
  writer.u32(request.maxRecords);
}

function readFetchRequest(reader: BinaryReader): FetchRequest {
  const topic = reader.string();
  const partition = reader.u32();
  const offset = reader.u64();
  const maxRecords = reader.u32();
  return { type: "fetch", topic, partition, offset, maxRecords };
}

function writeFetchResponse(writer: BinaryWriter, response: FetchResponse): void {
  writer.u64(response.logEndOffset);
  writer.u32(response.records.length);
  for (const record of response.records) {
    writer.u64(record.offset);
    writer.i64(record.timestamp);
    writer.nullableBytes(record.key);
    writer.bytes(record.value);
  }
}

function readFetchResponse(reader: BinaryReader): FetchResponse {
  const logEndOffset = reader.u64();
  const count = reader.u32();
  const records: LogRecord[] = [];
  for (let i = 0; i < count; i++) {
    const offset = reader.u64();
    const timestamp = reader.i64();
    const key = reader.nullableBytes();
    const value = reader.bytes();
    records.push({ offset, timestamp, key, value });
  }
  return { type: "fetch", logEndOffset, records };
}

// Not MALFORMED_MESSAGE: the bytes may be valid for a newer version, and this
// tells the client it could retry with an older one.
function unsupported(header: MessageHeader): ProtocolError {
  return new ProtocolError(
    "UNSUPPORTED_VERSION",
    `apiKey ${header.apiKey} version ${header.apiVersion} is not supported`,
  );
}

// Error messages can quote client input, such as a 65,535-byte topic name, which
// could push them past a string field's limit and leave the error unreportable.
// 1,000 characters is at most 4,000 bytes. Cutting by whole characters never
// breaks one in half.
function shorten(message: string): string {
  const characters = Array.from(message);
  if (characters.length <= MAX_ERROR_MESSAGE_LENGTH) {
    return message;
  }
  return characters.slice(0, MAX_ERROR_MESSAGE_LENGTH).join("") + "...";
}

// In the `default` of a switch that handles every message type. A new type that
// a switch forgets can reach it, and then this call stops compiling.
function unreachable(value: never): never {
  throw new Error(`unhandled message type: ${JSON.stringify(value)}`);
}
