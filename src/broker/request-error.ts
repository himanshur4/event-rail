// Errors from the broker's own request rules. There is one so far: a record must
// be small enough to be fetched. A produce request of 8 MiB could hold a record
// whose fetch response, with its extra fields, wouldn't fit in any frame, leaving
// every consumer of that partition stuck at that offset forever.

/** Machine-readable reasons the broker refused a request. */
export type RequestErrorCode = "RECORD_TOO_LARGE"; // a record's key + value exceed the broker's limit

export class RequestError extends Error {
  readonly code: RequestErrorCode;

  constructor(code: RequestErrorCode, message: string) {
    super(message);
    this.name = "RequestError";
    this.code = code;
  }
}
