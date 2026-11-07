// A command-line program to run a broker and talk to it. See USAGE below. It only
// reads arguments and prints results; the classes it calls do the real work.

import { parseArgs } from "node:util";

import { Admin } from "./client/admin.js";
import { Connection } from "./client/connection.js";
import { Consumer } from "./client/consumer.js";
import { Producer } from "./client/producer.js";
import { DEFAULT_HOST, DEFAULT_PORT } from "./common/network-defaults.js";
import { BrokerServer } from "./server/broker-server.js";
import { TopicManager } from "./topic/topic-manager.js";

const USAGE = `Usage: node dist/src/cli.js <command> [options]

Commands:
  broker [--data <dir>]                              run a broker (Ctrl+C to stop)
  create-topic <name> [--partitions <n>]             create a topic (default 1 partition)
  topics                                             list topics
  produce <topic> <value> [--key <key>]              send one record
  consume <topic> [--partition <n>] [--from <offset>] [--follow]
                                                     print records (all partitions by default)

Options for every command:
  --host <address>   broker address (default ${DEFAULT_HOST})
  --port <number>    broker port (default ${DEFAULT_PORT})`;

// How long `consume --follow` waits before asking again after a poll found nothing.
const FOLLOW_INTERVAL_MS = 500;

async function main(argv: string[]): Promise<void> {
  const { values, positionals } = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      host: { type: "string", default: DEFAULT_HOST },
      port: { type: "string", default: String(DEFAULT_PORT) },
      data: { type: "string", default: "./data" },
      partitions: { type: "string", default: "1" },
      partition: { type: "string" },
      key: { type: "string" },
      from: { type: "string", default: "0" },
      follow: { type: "boolean", default: false },
      help: { type: "boolean", short: "h", default: false },
    },
  });
  const [command, ...args] = positionals;
  const host = values.host;
  const port = parseInteger(values.port, "--port");

  if (values.help || command === undefined) {
    console.log(USAGE);
    return;
  }
  switch (command) {
    case "broker":
      return runBroker(values.data, host, port);
    case "create-topic": {
      const name = required(args[0], "a topic name");
      const partitions = parseInteger(values.partitions, "--partitions");
      await withConnection(host, port, (connection) => new Admin(connection).createTopic(name, partitions));
      console.log(`created topic ${name} with ${partitions} partition(s)`);
      return;
    }
    case "topics":
      return withConnection(host, port, async (connection) => {
        const topics = await new Admin(connection).listTopics();
        if (topics.length === 0) {
          console.log("no topics");
        }
        for (const topic of topics) {
          console.log(`${topic.name}\t${topic.partitionCount} partition(s)`);
        }
      });
    case "produce": {
      const topic = required(args[0], "a topic name");
      const value = required(args[1], "a value");
      const key = values.key === undefined ? null : Buffer.from(values.key);
      return withConnection(host, port, async (connection) => {
        const [stored] = await new Producer(connection).send(topic, [{ key, value: Buffer.from(value) }]);
        console.log(`stored in ${topic} partition ${stored!.partition} at offset ${stored!.offset}`);
      });
    }
    case "consume": {
      const topic = required(args[0], "a topic name");
      const from = parseInteger(values.from, "--from");
      const partition = values.partition === undefined ? null : parseInteger(values.partition, "--partition");
      return withConnection(host, port, (connection) => consume(connection, topic, partition, from, values.follow));
    }
    default:
      throw new Error(`unknown command "${command}"\n\n${USAGE}`);
  }
}

// Runs until Ctrl+C. The server closes first, so no request arrives after storage has closed.
async function runBroker(dataDir: string, host: string, port: number): Promise<void> {
  const topics = await TopicManager.open(dataDir);
  for (const recovery of topics.recoveredTails) {
    console.log(
      `recovered ${recovery.topic} partition ${recovery.partition}: ` +
        `discarded ${recovery.tail.discardedBytes} bytes of a torn write (${recovery.tail.reason})`,
    );
  }
  const log = (line: string): void => console.error(`${new Date().toISOString()} ${line}`);
  const server = await BrokerServer.start(topics, { host, port, log });
  console.log(`EventRail broker listening on ${server.host}:${server.port}, data in ${dataDir}`);

  await new Promise<void>((resolve) => {
    process.once("SIGINT", () => resolve());
    process.once("SIGTERM", () => resolve());
  });
  console.log("shutting down: answering requests in progress, then closing storage");
  await server.close();
  await topics.close();
  console.log("stopped");
}

// Reads every partition (or one) from `from`, printing each record. Stops once a
// full round finds nothing, unless `follow` is set.
async function consume(
  connection: Connection,
  topic: string,
  partition: number | null,
  from: number,
  follow: boolean,
): Promise<void> {
  let partitions: number[];
  if (partition === null) {
    const info = (await new Admin(connection).listTopics()).find((t) => t.name === topic);
    if (info === undefined) {
      throw new Error(`topic ${topic} does not exist`);
    }
    partitions = Array.from({ length: info.partitionCount }, (_, i) => i);
  } else {
    partitions = [partition];
  }
  const consumers = partitions.map((p) => new Consumer(connection, topic, p, { fromOffset: from }));

  for (;;) {
    let received = 0;
    for (const consumer of consumers) {
      const records = await consumer.poll();
      for (const record of records) {
        const key = record.key === null ? "(none)" : record.key.toString();
        console.log(`${topic}/${consumer.partition} offset ${record.offset}  key=${key}  value=${record.value.toString()}`);
      }
      received += records.length;
    }
    if (received === 0) {
      if (!follow) {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, FOLLOW_INTERVAL_MS));
    }
  }
}

async function withConnection<T>(host: string, port: number, body: (connection: Connection) => Promise<T>): Promise<T> {
  const connection = await Connection.connect({ host, port });
  try {
    return await body(connection);
  } finally {
    await connection.close();
  }
}

function parseInteger(text: string, name: string): number {
  const value = Number(text);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative whole number, got "${text}"`);
  }
  return value;
}

function required(value: string | undefined, what: string): string {
  if (value === undefined) {
    throw new Error(`missing ${what}\n\n${USAGE}`);
  }
  return value;
}

main(process.argv.slice(2)).catch((error: unknown) => {
  const name = error instanceof Error && "code" in error ? `${String(error.code)}: ` : "";
  console.error(`error: ${name}${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
});
