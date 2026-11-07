// The client's side of one TCP connection to a broker. Many requests can be in
// flight at once, so `pending` maps each correlation ID to the request waiting for
// that answer. A late answer finds its ID gone from the map and is ignored, so it
// can never be mistaken for the answer to a different request.

import { connect, type Socket } from "node:net";

import { decodeHeader, decodeResponse, encodeRequest } from "../protocol/codec.js";
import { DEFAULT_MAX_FRAME_BYTES, FrameDecoder, encodeFrame } from "../protocol/frame.js";
import { API_KEYS, type ApiName, type ApiRequest, type SuccessResponse } from "../protocol/messages.js";
import { DEFAULT_HOST, DEFAULT_PORT } from "../common/network-defaults.js";
import { BrokerError, ConnectionError } from "./client-errors.js";

export interface ConnectionOptions {
  readonly host?: string;
  readonly port?: number;
  /** How long to wait for each answer before giving up (default 30 s). */
  readonly requestTimeoutMs?: number;
  /** Largest frame sent or accepted (default 8 MiB, matching the broker). */
  readonly maxFrameBytes?: number;
}

/**
 * The success response type for a request type, e.g. ResponseFor<FetchRequest>
 * is FetchResponse.
 */
export type ResponseFor<R extends ApiRequest> = Extract<SuccessResponse, { type: R["type"] }>;

interface PendingRequest {
  readonly type: ApiName;
  readonly resolve: (response: SuccessResponse) => void;
  readonly reject: (error: Error) => void;
  readonly timer: NodeJS.Timeout;
}

const MAX_CORRELATION_ID = 0xffff_ffff;

/** One connection to a broker. Requests may be sent concurrently. */
export class Connection {
  private readonly socket: Socket;
  private readonly decoder: FrameDecoder;
  private readonly requestTimeoutMs: number;
  private readonly maxFrameBytes: number;
  private readonly pending = new Map<number, PendingRequest>();
  private nextCorrelationId = 0;
  // Set once the connection can no longer be used; every later send() rejects with it.
  private failure: Error | null = null;
  // Set by close(): no new requests, but answers to sent ones still arrive.
  private closeRequested = false;

  /** Resolves once the connection has fully closed. */
  readonly closed: Promise<void>;

  private constructor(socket: Socket, options: ConnectionOptions) {
    this.socket = socket;
    this.maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    this.decoder = new FrameDecoder(this.maxFrameBytes);
    this.requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    this.closed = new Promise((resolve) => socket.once("close", () => resolve()));

    socket.setNoDelay(true);
    socket.on("data", (chunk: Buffer) => this.onData(chunk));
    socket.on("error", (error) => {
      this.fail(new ConnectionError("CONNECTION_CLOSED", `connection to the broker failed: ${error.message}`));
    });
    socket.on("close", () => {
      this.fail(new ConnectionError("CONNECTION_CLOSED", "the connection to the broker closed"));
    });
  }

  /** Opens a connection. Rejects if the broker can't be reached. */
  static connect(options: ConnectionOptions = {}): Promise<Connection> {
    return new Promise((resolve, reject) => {
      const socket = connect({ host: options.host ?? DEFAULT_HOST, port: options.port ?? DEFAULT_PORT });
      socket.once("error", reject);
      socket.once("connect", () => {
        socket.off("error", reject);
        resolve(new Connection(socket, options));
      });
    });
  }

  /**
   * Sends a request and resolves with its success response. Rejects with
   * BrokerError if the broker answers with an error, or ConnectionError if
   * no answer arrives (closed, timed out, or invalid).
   */
  send<R extends ApiRequest>(request: R): Promise<ResponseFor<R>> {
    return new Promise((resolve, reject) => {
      if (this.failure !== null) {
        reject(this.failure);
        return;
      }
      if (this.closeRequested) {
        reject(new ConnectionError("CONNECTION_CLOSED", "close() was called on this connection"));
        return;
      }
      const correlationId = this.takeCorrelationId();
      let frame: Buffer;
      try {
        frame = encodeFrame(encodeRequest(correlationId, request), this.maxFrameBytes);
      } catch (error) {
        // Nothing was sent, so only this request fails; the connection is fine.
        reject(error);
        return;
      }
      const timer = setTimeout(() => {
        this.pending.delete(correlationId);
        reject(new ConnectionError("REQUEST_TIMEOUT", `no answer to ${request.type} within ${this.requestTimeoutMs} ms`));
      }, this.requestTimeoutMs);
      this.pending.set(correlationId, {
        type: request.type,
        // Safe: onResponse has checked the answer is for this API.
        resolve: (response) => resolve(response as ResponseFor<R>),
        reject,
        timer,
      });
      this.socket.write(frame);
    });
  }

  /**
   * Closes the connection. Requests already sent still get their answers
   * first; new ones are refused.
   */
  async close(): Promise<void> {
    this.closeRequested = true;
    this.socket.end();
    await this.closed;
  }

  // Wraps back to 0 after the largest u32; a collision would need 4 billion
  // requests in flight at once.
  private takeCorrelationId(): number {
    const id = this.nextCorrelationId;
    this.nextCorrelationId = id === MAX_CORRELATION_ID ? 0 : id + 1;
    return id;
  }

  private onData(chunk: Buffer): void {
    let frames: Buffer[];
    try {
      frames = this.decoder.push(chunk);
    } catch (error) {
      this.fail(new ConnectionError("PROTOCOL_VIOLATION", `the broker sent an invalid frame: ${(error as Error).message}`));
      return;
    }
    for (const frame of frames) {
      try {
        this.onResponse(frame);
      } catch (error) {
        // A response we can't read means this connection can't be trusted at all.
        const message = error instanceof ConnectionError ? error.message : `the broker sent an invalid response: ${(error as Error).message}`;
        this.fail(new ConnectionError("PROTOCOL_VIOLATION", message));
        return;
      }
    }
  }

  private onResponse(frame: Buffer): void {
    const header = decodeHeader(frame);
    const pending = this.pending.get(header.correlationId);
    if (pending === undefined) {
      // The answer to a request that already timed out.
      return;
    }
    if (header.apiKey !== API_KEYS[pending.type]) {
      throw new ConnectionError(
        "PROTOCOL_VIOLATION",
        `the answer to ${pending.type} request ${header.correlationId} has apiKey ${header.apiKey}`,
      );
    }
    const response = decodeResponse(frame);
    this.pending.delete(header.correlationId);
    clearTimeout(pending.timer);
    if (response.type === "error") {
      pending.reject(new BrokerError(response.code, response.message));
    } else {
      pending.resolve(response);
    }
  }

  // Every waiting request and every later send() rejects with the first failure.
  private fail(error: Error): void {
    if (this.failure === null) {
      this.failure = error;
    }
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(this.failure);
    }
    this.pending.clear();
    this.socket.destroy();
  }
}
