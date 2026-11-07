// The broker's side of one client connection. Requests run one at a time, in
// arrival order, so a producer's batch A is stored before its batch B; different
// connections still run side by side.

import type { Socket } from "node:net";

import { errorResponseFor, type RequestHandler } from "../broker/request-handler.js";
import { decodeHeader, decodeRequest, encodeResponse } from "../protocol/codec.js";
import { FrameDecoder, encodeFrame } from "../protocol/frame.js";
import type { ApiResponse, MessageHeader } from "../protocol/messages.js";

export interface ServerConnectionOptions {
  readonly maxFrameBytes: number;
  /** Close the connection after this long with no traffic; 0 disables it. */
  readonly idleTimeoutMs: number;
  /** Stop reading from the socket while this many requests are waiting. */
  readonly maxInFlightRequests: number;
  readonly log: (message: string) => void;
}

/** Serves one client connection until it closes. */
export class ServerConnection {
  private readonly socket: Socket;
  private readonly handler: RequestHandler;
  private readonly options: ServerConnectionOptions;
  private readonly decoder: FrameDecoder;
  // Saved now, because Node forgets the address once the socket closes.
  private readonly peer: string;
  // Each new request waits for this promise, then becomes the new end of the chain.
  private queue: Promise<void> = Promise.resolve();
  private inFlight = 0;
  private closing = false;

  /** Resolves once the socket has fully closed, for any reason. */
  readonly closed: Promise<void>;

  constructor(socket: Socket, handler: RequestHandler, options: ServerConnectionOptions) {
    this.socket = socket;
    this.handler = handler;
    this.options = options;
    this.decoder = new FrameDecoder(options.maxFrameBytes);
    this.peer = `${socket.remoteAddress}:${socket.remotePort}`;
    this.closed = new Promise((resolve) => socket.once("close", () => resolve()));

    // Small responses shouldn't wait to be combined with later writes.
    socket.setNoDelay(true);
    socket.setTimeout(options.idleTimeoutMs);

    socket.on("data", (chunk: Buffer) => this.onData(chunk));
    socket.on("end", () => this.onEnd());
    socket.on("timeout", () => this.onTimeout());
    // Without a listener, a socket error would crash the whole broker.
    socket.on("error", (error) => this.options.log(`connection ${this.peer}: ${error.message}`));
    socket.on("close", () => {
      // A partial request never runs, like a torn write in crash recovery.
      if (this.decoder.bufferedBytes > 0) {
        this.options.log(
          `connection ${this.peer} closed in the middle of a request; ${this.decoder.bufferedBytes} bytes discarded`,
        );
      }
    });
  }

  /**
   * Stops reading requests, finishes the ones already received, then closes
   * the connection. If the client hasn't closed its side within `graceMs`,
   * the connection is cut.
   */
  async close(graceMs: number): Promise<void> {
    this.closing = true;
    this.socket.pause();
    await this.queue;
    this.socket.end();
    const timer = setTimeout(() => this.socket.destroy(), graceMs);
    await this.closed;
    clearTimeout(timer);
  }

  private onData(chunk: Buffer): void {
    if (this.closing) {
      return;
    }
    let frames: Buffer[];
    try {
      frames = this.decoder.push(chunk);
    } catch (error) {
      // The next frame's start is unknown and there is no header to answer.
      this.options.log(`connection ${this.peer}: ${(error as Error).message}; closing it`);
      this.socket.destroy();
      return;
    }
    for (const frame of frames) {
      this.enqueue(frame);
    }
  }

  private enqueue(frame: Buffer): void {
    this.inFlight += 1;
    // Back-pressure: TCP then fills up and the client's own writes slow down.
    if (this.inFlight >= this.options.maxInFlightRequests) {
      this.socket.pause();
    }
    // process() never rejects, so one failure can't break the chain.
    this.queue = this.queue.then(() => this.process(frame));
  }

  private async process(frame: Buffer): Promise<void> {
    try {
      let header: MessageHeader;
      try {
        header = decodeHeader(frame);
      } catch {
        // No correlation ID to answer, and the client would wait forever for a
        // reply, so closing tells it at once.
        this.options.log(`connection ${this.peer}: request too short for a header; closing it`);
        this.socket.destroy();
        return;
      }
      const response = await this.answer(frame);
      await this.write(this.responseFrame(header, response));
    } catch (error) {
      this.options.log(`connection ${this.peer}: unexpected error: ${(error as Error).message}`);
      this.socket.destroy();
    } finally {
      this.inFlight -= 1;
      if (!this.closing && this.socket.isPaused() && this.inFlight < this.options.maxInFlightRequests) {
        this.socket.resume();
      }
    }
  }

  // The header is known by now, so every failure becomes an error response and
  // the connection stays open.
  private async answer(frame: Buffer): Promise<ApiResponse> {
    try {
      const request = decodeRequest(frame);
      return await this.handler.handle(request);
    } catch (error) {
      if (errorResponseFor(error).code === "UNKNOWN_SERVER_ERROR") {
        this.options.log(`connection ${this.peer}: unexpected error: ${(error as Error).message}`);
      }
      return errorResponseFor(error);
    }
  }

  // If a success response can't be encoded, the client still deserves an answer,
  // and an error response can always be encoded.
  private responseFrame(header: MessageHeader, response: ApiResponse): Buffer {
    try {
      return encodeFrame(encodeResponse(header, response), this.options.maxFrameBytes);
    } catch (error) {
      this.options.log(`connection ${this.peer}: could not encode a response: ${(error as Error).message}`);
      return encodeFrame(encodeResponse(header, errorResponseFor(error)), this.options.maxFrameBytes);
    }
  }

  private async write(frame: Buffer): Promise<void> {
    // The client may have left while we worked; the request's effect still stands.
    if (this.socket.destroyed || !this.socket.writable) {
      return;
    }
    // A full outgoing buffer means the client isn't reading. Wait before
    // processing more, so its answers can't pile up in the broker's memory.
    if (!this.socket.write(frame)) {
      await new Promise<void>((resolve) => {
        const done = (): void => {
          this.socket.off("drain", done);
          this.socket.off("close", done);
          resolve();
        };
        this.socket.on("drain", done);
        this.socket.on("close", done);
      });
    }
  }

  // The client has finished sending but may still be owed answers.
  private onEnd(): void {
    void this.queue.then(() => this.socket.end());
  }

  private onTimeout(): void {
    if (this.inFlight === 0) {
      this.options.log(`connection ${this.peer}: idle for ${this.options.idleTimeoutMs} ms; closing it`);
      this.socket.destroy();
    } else {
      // Not idle, just waiting on slow storage: check again later.
      this.socket.setTimeout(this.options.idleTimeoutMs);
    }
  }
}
