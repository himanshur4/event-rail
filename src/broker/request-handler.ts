// What the broker does with each request: turns a decoded request into calls on
// TopicManager and the partition logs. It knows nothing about sockets or bytes,
// so it can be tested without a network.

import type { LogRecord } from "../common/types.js";
import { FETCH_RESPONSE_BASE_BYTES, fetchRecordBytes } from "../protocol/codec.js";
import { DEFAULT_MAX_FRAME_BYTES } from "../protocol/frame.js";
import type {
  ApiRequest,
  ErrorResponse,
  FetchRequest,
  FetchResponse,
  MetadataResponse,
  ProduceRequest,
  ProduceResponse,
  SuccessResponse,
} from "../protocol/messages.js";
import { ProtocolError } from "../protocol/protocol-error.js";
import { LogError, validateReadArguments } from "../storage/log.js";
import type { TopicManager } from "../topic/topic-manager.js";
import { TopicError } from "../topic/topic-metadata.js";
import { RequestError } from "./request-error.js";

/** Largest record the broker accepts by default: key + value, 1 MiB. */
export const DEFAULT_MAX_RECORD_BYTES = 1024 * 1024;

export interface RequestHandlerOptions {
  /** Largest key + value of one record, in bytes (default 1 MiB). */
  readonly maxRecordBytes?: number;
  /** Largest fetch response payload, in bytes (default: the default frame limit, 8 MiB). */
  readonly maxResponseBytes?: number;
}

// What a fetch response holding one record needs besides its key and value.
const ONE_RECORD_OVERHEAD =
  FETCH_RESPONSE_BASE_BYTES + fetchRecordBytes({ offset: 0, timestamp: 0, key: null, value: Buffer.alloc(0) });

/**
 * Carries out decoded requests against a TopicManager. Errors are thrown as
 * the storage and topic layers raise them; errorResponseFor turns them into
 * error responses.
 */
export class RequestHandler {
  private readonly topics: TopicManager;
  private readonly maxRecordBytes: number;
  private readonly maxResponseBytes: number;
  private readonly readChunk: number;

  constructor(topics: TopicManager, options: RequestHandlerOptions = {}) {
    this.topics = topics;
    this.maxRecordBytes = options.maxRecordBytes ?? DEFAULT_MAX_RECORD_BYTES;
    this.maxResponseBytes = options.maxResponseBytes ?? DEFAULT_MAX_FRAME_BYTES;
    if (!Number.isSafeInteger(this.maxRecordBytes) || this.maxRecordBytes < 0) {
      throw new RangeError(`maxRecordBytes must be a non-negative integer, got ${this.maxRecordBytes}`);
    }
    // Every stored record must be fetchable, so a maximum-size record has to fit
    // in a response.
    if (this.maxRecordBytes + ONE_RECORD_OVERHEAD > this.maxResponseBytes) {
      throw new RangeError(
        `maxRecordBytes ${this.maxRecordBytes} plus ${ONE_RECORD_OVERHEAD} bytes of fetch response ` +
          `fields must fit in maxResponseBytes ${this.maxResponseBytes}`,
      );
    }
    // Reading `maxRecords` at once could load 500 MiB of records only to send
    // 8 MiB. Reading this many at a time keeps memory near one response's size.
    this.readChunk = Math.max(1, Math.floor(this.maxResponseBytes / (this.maxRecordBytes + ONE_RECORD_OVERHEAD)));
  }

  /**
   * Carries out one request and returns its success response. Throws
   * TopicError, LogError or RequestError when the request can't be done.
   */
  async handle(request: ApiRequest): Promise<SuccessResponse> {
    switch (request.type) {
      case "createTopic":
        await this.topics.createTopic(request.topic, request.partitionCount);
        return { type: "createTopic" };
      case "metadata":
        return this.metadata();
      case "produce":
        return this.produce(request);
      case "fetch":
        return this.fetch(request);
      default: {
        const unhandled: never = request;
        throw new Error(`unhandled request ${JSON.stringify(unhandled)}`);
      }
    }
  }

  private metadata(): MetadataResponse {
    const topics = this.topics.listTopics().map((topic) => ({
      name: topic.name,
      partitionCount: topic.partitionCount,
    }));
    return { type: "metadata", topics };
  }

  private async produce(request: ProduceRequest): Promise<ProduceResponse> {
    const log = this.topics.partition(request.topic, request.partition);
    // Checked before anything is appended: a batch is stored whole or not at all.
    for (const [index, record] of request.records.entries()) {
      const size = (record.key?.length ?? 0) + record.value.length;
      if (size > this.maxRecordBytes) {
        throw new RequestError(
          "RECORD_TOO_LARGE",
          `record ${index} has ${size} bytes of key and value; the limit is ${this.maxRecordBytes}`,
        );
      }
    }
    const { baseOffset, lastOffset } = await log.append(request.records);
    return { type: "produce", baseOffset, lastOffset };
  }

  private async fetch(request: FetchRequest): Promise<FetchResponse> {
    const log = this.topics.partition(request.topic, request.partition);
    // Without this, maxRecords 0 would skip the loop and return [], which a
    // consumer would take to mean it has caught up.
    validateReadArguments(request.offset, request.maxRecords, log.logEndOffset);

    const records: LogRecord[] = [];
    let responseBytes = FETCH_RESPONSE_BASE_BYTES;
    let nextOffset = request.offset;
    let full = false;

    while (!full && records.length < request.maxRecords) {
      const wanted = Math.min(this.readChunk, request.maxRecords - records.length);
      const chunk = await log.read(nextOffset, wanted);
      if (chunk.length === 0) {
        break;
      }
      for (const record of chunk) {
        const recordBytes = fetchRecordBytes(record);
        // The first record is always included, so a consumer can never get stuck.
        if (records.length > 0 && responseBytes + recordBytes > this.maxResponseBytes) {
          full = true;
          break;
        }
        records.push(record);
        responseBytes += recordBytes;
      }
      nextOffset = chunk.at(-1)!.offset + 1;
    }

    // Read after the records: appends may have happened meanwhile, and an earlier
    // value could be smaller than the offset of a record being returned.
    return { type: "fetch", logEndOffset: log.logEndOffset, records };
  }
}

/**
 * The error response for an error thrown while handling a request. The
 * broker's own errors keep their code and message; anything else is a bug,
 * reported as UNKNOWN_SERVER_ERROR without its details.
 */
export function errorResponseFor(error: unknown): ErrorResponse {
  if (
    error instanceof TopicError ||
    error instanceof LogError ||
    error instanceof ProtocolError ||
    error instanceof RequestError
  ) {
    return { type: "error", code: error.code, message: error.message };
  }
  // An unexpected error's message may contain internal details, such as file paths.
  return { type: "error", code: "UNKNOWN_SERVER_ERROR", message: "unexpected broker error" };
}
