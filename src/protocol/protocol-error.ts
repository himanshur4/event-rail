// The error for bytes from the network that break the wire protocol. They may
// come from a buggy client or a program connected to the wrong port, so a failure
// must only affect that one connection.
//
// A frame too large, or too short to hold a header, closes the connection: the
// server can't tell where the next frame starts, or who to answer. Anything after
// the header was read gets an error response and the connection stays open.

/** Machine-readable reasons a peer's bytes were rejected. */
export type ProtocolErrorCode =
  | "FRAME_TOO_LARGE" // a frame's length prefix is larger than the allowed maximum
  | "MALFORMED_MESSAGE" // a frame's contents don't decode (too short, bad lengths, bad text...)
  | "UNSUPPORTED_VERSION"; // a message's apiKey or apiVersion is one this code can't read

export class ProtocolError extends Error {
  readonly code: ProtocolErrorCode;

  constructor(code: ProtocolErrorCode, message: string) {
    super(message);
    this.name = "ProtocolError";
    this.code = code;
  }
}
