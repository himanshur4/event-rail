// Listens on a TCP port and gives each client connection its own ServerConnection.
// Connections share one RequestHandler and one TopicManager, so there is one copy
// of the data, but each has its own decoder and queue, so one misbehaving client
// can't mix up another's requests.
//
// The server doesn't own the TopicManager: whoever opened it closes it, after the
// server, so storage stays open until the final request has been answered.

import { createServer, type AddressInfo, type Server, type Socket } from "node:net";

import { DEFAULT_MAX_RECORD_BYTES, RequestHandler } from "../broker/request-handler.js";
import { DEFAULT_HOST, DEFAULT_PORT } from "../common/network-defaults.js";
import { DEFAULT_MAX_FRAME_BYTES } from "../protocol/frame.js";
import type { TopicManager } from "../topic/topic-manager.js";
import { ServerConnection } from "./server-connection.js";

export interface BrokerServerOptions {
  readonly host?: string;
  /** TCP port (default 7400). 0 lets the operating system pick a free one, as tests do. */
  readonly port?: number;
  /** Connections beyond this many are closed as soon as they arrive (default 1000). */
  readonly maxConnections?: number;
  /** A connection with no traffic for this long is closed (default 10 minutes; 0 disables). */
  readonly idleTimeoutMs?: number;
  /** Largest frame accepted or sent (default 8 MiB). */
  readonly maxFrameBytes?: number;
  /** Largest key + value of one record (default 1 MiB). */
  readonly maxRecordBytes?: number;
  /** Requests a connection may have waiting before it stops reading (default 32). */
  readonly maxInFlightRequests?: number;
  /** How long close() waits for each client to close its side before cutting it (default 5 s). */
  readonly shutdownGraceMs?: number;
  /** Receives one line per notable event; ignored by default. */
  readonly log?: (message: string) => void;
}

/** A running broker: a TCP server in front of one TopicManager. */
export class BrokerServer {
  private readonly server: Server;
  private readonly handler: RequestHandler;
  private readonly options: Required<BrokerServerOptions>;
  private readonly connections = new Set<ServerConnection>();
  // The shutdown in progress, so close() can be called twice safely.
  private closing: Promise<void> | null = null;

  // Private: starting needs `await`, so callers use start().
  private constructor(server: Server, handler: RequestHandler, options: Required<BrokerServerOptions>) {
    this.server = server;
    this.handler = handler;
    this.options = options;
  }

  /**
   * Starts listening and serving requests against `topics`. Rejects if the
   * port can't be used (for example, another program already has it).
   */
  static async start(topics: TopicManager, options: BrokerServerOptions = {}): Promise<BrokerServer> {
    const resolved: Required<BrokerServerOptions> = {
      host: options.host ?? DEFAULT_HOST,
      port: options.port ?? DEFAULT_PORT,
      maxConnections: options.maxConnections ?? 1000,
      idleTimeoutMs: options.idleTimeoutMs ?? 10 * 60 * 1000,
      maxFrameBytes: options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
      maxRecordBytes: options.maxRecordBytes ?? DEFAULT_MAX_RECORD_BYTES,
      maxInFlightRequests: options.maxInFlightRequests ?? 32,
      shutdownGraceMs: options.shutdownGraceMs ?? 5000,
      log: options.log ?? (() => {}),
    };
    const handler = new RequestHandler(topics, {
      maxRecordBytes: resolved.maxRecordBytes,
      maxResponseBytes: resolved.maxFrameBytes,
    });
    // allowHalfOpen: a client that has finished sending may still be owed answers,
    // so don't close our side automatically.
    const server = createServer({ allowHalfOpen: true });
    const broker = new BrokerServer(server, handler, resolved);
    server.on("connection", (socket: Socket) => broker.accept(socket));

    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(resolved.port, resolved.host, () => {
        server.off("error", reject);
        resolve();
      });
    });
    server.on("error", (error) => resolved.log(`server: ${error.message}`));
    return broker;
  }

  /** The port actually listened on (useful when started with port 0). */
  get port(): number {
    return (this.server.address() as AddressInfo).port;
  }

  /** The address listened on. */
  get host(): string {
    return this.options.host;
  }

  /** Connections currently open. */
  get connectionCount(): number {
    return this.connections.size;
  }

  /**
   * Stops accepting connections, answers every request already received, and
   * closes all connections. Does not close the TopicManager. Safe to call twice.
   */
  close(): Promise<void> {
    if (this.closing === null) {
      this.closing = this.closeNow();
    }
    return this.closing;
  }

  private accept(socket: Socket): void {
    // Each connection holds memory and a file descriptor, so their number has a ceiling.
    if (this.closing !== null || this.connections.size >= this.options.maxConnections) {
      this.options.log(`refused a connection from ${socket.remoteAddress}: ${this.closing ? "shutting down" : "too many connections"}`);
      socket.destroy();
      return;
    }
    const connection = new ServerConnection(socket, this.handler, {
      maxFrameBytes: this.options.maxFrameBytes,
      idleTimeoutMs: this.options.idleTimeoutMs,
      maxInFlightRequests: this.options.maxInFlightRequests,
      log: this.options.log,
    });
    this.connections.add(connection);
    void connection.closed.then(() => this.connections.delete(connection));
  }

  private async closeNow(): Promise<void> {
    // Stops accepting at once; the callback runs when every connection has closed.
    const stopped = new Promise<void>((resolve) => this.server.close(() => resolve()));
    await Promise.all([...this.connections].map((connection) => connection.close(this.options.shutdownGraceMs)));
    await stopped;
  }
}
