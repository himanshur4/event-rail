// Length-prefixed frames. TCP delivers a stream of bytes without message
// boundaries, so every message travels as a 4-byte big-endian length followed by
// exactly that many payload bytes.

import { ProtocolError } from "./protocol-error.js";

/** Bytes in a frame's length prefix. */
export const FRAME_HEADER_SIZE = 4;

/** Largest frame payload accepted by default: 8 MiB. */
export const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;

const MAX_UINT32 = 0xffff_ffff;

/**
 * Wraps `payload` in a frame: its 4-byte big-endian length, then the payload.
 * Throws FRAME_TOO_LARGE if the payload exceeds `maxFrameBytes`.
 */
export function encodeFrame(payload: Buffer, maxFrameBytes = DEFAULT_MAX_FRAME_BYTES): Buffer {
  if (payload.length > maxFrameBytes) {
    throw new ProtocolError(
      "FRAME_TOO_LARGE",
      `frame payload of ${payload.length} bytes exceeds the ${maxFrameBytes}-byte limit`,
    );
  }
  const frame = Buffer.alloc(FRAME_HEADER_SIZE + payload.length);
  frame.writeUInt32BE(payload.length, 0);
  payload.copy(frame, FRAME_HEADER_SIZE);
  return frame;
}

/**
 * Turns the chunks received on one connection back into whole frame payloads.
 * Create one decoder per connection, and pass it every chunk, in order.
 */
// A two-state machine: read the 4 length bytes into `header`, then fill `payload`.
// Each received byte is copied exactly once, however the stream was split.
export class FrameDecoder {
  private readonly maxFrameBytes: number;
  private readonly header = Buffer.alloc(FRAME_HEADER_SIZE);
  private headerFilled = 0;
  // Null while the length is still being read.
  private payload: Buffer | null = null;
  private payloadFilled = 0;
  // Permanent: after a protocol violation, the next frame's start is unknown.
  private failure: ProtocolError | null = null;

  constructor(maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {
    if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 0 || maxFrameBytes > MAX_UINT32) {
      throw new RangeError(`maxFrameBytes must be between 0 and ${MAX_UINT32}, got ${maxFrameBytes}`);
    }
    this.maxFrameBytes = maxFrameBytes;
  }

  /** Bytes received that belong to a frame that isn't complete yet. */
  get bufferedBytes(): number {
    return this.headerFilled + this.payloadFilled;
  }

  /**
   * Accepts the next chunk received on the connection and returns every frame
   * payload it completes, in order (often none, sometimes several). Throws
   * FRAME_TOO_LARGE as soon as a frame announces a length above the maximum.
   */
  push(chunk: Buffer): Buffer[] {
    if (this.failure !== null) {
      throw this.failure;
    }
    const frames: Buffer[] = [];
    let position = 0;

    while (position < chunk.length) {
      if (this.payload === null) {
        const count = Math.min(FRAME_HEADER_SIZE - this.headerFilled, chunk.length - position);
        chunk.copy(this.header, this.headerFilled, position, position + count);
        this.headerFilled += count;
        position += count;
        if (this.headerFilled < FRAME_HEADER_SIZE) {
          break;
        }

        // The length is untrusted: check it before reserving any memory for it.
        const length = this.header.readUInt32BE(0);
        if (length > this.maxFrameBytes) {
          this.failure = new ProtocolError(
            "FRAME_TOO_LARGE",
            `frame of ${length} bytes exceeds the ${this.maxFrameBytes}-byte limit`,
          );
          this.headerFilled = 0;
          throw this.failure;
        }
        this.payload = Buffer.alloc(length);
        this.payloadFilled = 0;
      }

      const count = Math.min(this.payload.length - this.payloadFilled, chunk.length - position);
      chunk.copy(this.payload, this.payloadFilled, position, position + count);
      this.payloadFilled += count;
      position += count;

      // A zero-length frame completes here immediately, with nothing to copy.
      if (this.payloadFilled === this.payload.length) {
        frames.push(this.payload);
        this.payload = null;
        this.payloadFilled = 0;
        this.headerFilled = 0;
      }
    }
    return frames;
  }
}
