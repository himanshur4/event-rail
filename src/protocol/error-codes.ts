// The number sent on the wire for every error a response can report. Clients
// rely on these numbers, so a number never changes or gets reused; a new error
// takes the next free number.

import type { RequestErrorCode } from "../broker/request-error.js";
import type { LogErrorCode } from "../storage/log.js";
import type { TopicErrorCode } from "../topic/topic-metadata.js";
import type { ProtocolErrorCode } from "./protocol-error.js";

/** Every error a response can report: each layer's codes, plus UNKNOWN_SERVER_ERROR. */
export type ErrorCode =
  | "UNKNOWN_SERVER_ERROR"
  | ProtocolErrorCode
  | TopicErrorCode
  | LogErrorCode
  | RequestErrorCode;

/** The wire number for "no error": the request succeeded. */
export const NO_ERROR = 0;

// Typed so that a new error code doesn't compile until it has a number here.
const WIRE_NUMBERS: Readonly<Record<ErrorCode, number>> = {
  UNKNOWN_SERVER_ERROR: 1,

  FRAME_TOO_LARGE: 2,
  MALFORMED_MESSAGE: 3,
  UNSUPPORTED_VERSION: 4,

  INVALID_TOPIC_NAME: 5,
  INVALID_PARTITION_COUNT: 6,
  TOPIC_EXISTS: 7,
  UNKNOWN_TOPIC: 8,
  UNKNOWN_PARTITION: 9,
  CORRUPT_TOPIC: 10,
  MANAGER_CLOSED: 11,

  EMPTY_BATCH: 12,
  INVALID_OFFSET: 13,
  OFFSET_OUT_OF_RANGE: 14,
  INVALID_MAX_RECORDS: 15,
  LOG_CLOSED: 16,
  CORRUPT_LOG: 17,

  RECORD_TOO_LARGE: 18,
};

const CODES_BY_NUMBER = new Map<number, ErrorCode>();
for (const code of Object.keys(WIRE_NUMBERS) as ErrorCode[]) {
  CODES_BY_NUMBER.set(WIRE_NUMBERS[code], code);
}

/** The number sent on the wire for `code`. */
export function errorCodeToWire(code: ErrorCode): number {
  return WIRE_NUMBERS[code];
}

/**
 * The error code for a number read from the wire. A number this version doesn't
 * know, from a newer broker, becomes UNKNOWN_SERVER_ERROR; the message sent with
 * it still explains what went wrong.
 */
export function errorCodeFromWire(wireNumber: number): ErrorCode {
  return CODES_BY_NUMBER.get(wireNumber) ?? "UNKNOWN_SERVER_ERROR";
}
