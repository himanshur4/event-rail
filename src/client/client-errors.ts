// The two kinds of failure a client can see. The difference matters for retries:
// after a BrokerError nothing was stored, but after a ConnectionError the records
// may already be on disk, so retrying can store them twice (at-least-once delivery).

import type { ErrorCode } from "../protocol/error-codes.js";

/** The broker answered a request with an error response. */
export class BrokerError extends Error {
  /** The same code the broker raised internally, e.g. "UNKNOWN_TOPIC". */
  readonly code: ErrorCode;

  constructor(code: ErrorCode, message: string) {
    super(message);
    this.name = "BrokerError";
    this.code = code;
  }
}

/** Why a request got no answer from the broker. */
export type ConnectionErrorCode =
  | "CONNECTION_CLOSED" // the connection ended before the answer arrived
  | "REQUEST_TIMEOUT" // no answer within the request timeout
  | "PROTOCOL_VIOLATION"; // the broker sent something that isn't a valid answer

/** A request got no answer; it may or may not have been carried out. */
export class ConnectionError extends Error {
  readonly code: ConnectionErrorCode;

  constructor(code: ConnectionErrorCode, message: string) {
    super(message);
    this.name = "ConnectionError";
    this.code = code;
  }
}
