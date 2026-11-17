// A command-line program to run a broker and talk to it. See USAGE below. It only
// reads arguments and prints results; the classes it calls do the real work.

import { join } from "node:path";
import { parseArgs } from "node:util";

import { Admin } from "./client/admin.js";
import { Connection } from "./client/connection.js";
import { Consumer } from "./client/consumer.js";
import { GroupConsumer } from "./client/group-consumer.js";
import { Producer } from "./client/producer.js";
import type { LogRecord } from "./common/types.js";
import { DEFAULT_HOST, DEFAULT_PORT } from "./common/network-defaults.js";
import { GroupCoordinator } from "./group/group-coordinator.js";
import { OffsetStore } from "./group/offset-store.js";
import { BrokerServer } from "./server/broker-server.js";
import { TopicManager } from "./topic/topic-manager.js";

const USAGE = `Usage: node dist/src/cli.js <command> [options]

Commands:
  broker [--data <dir>]                              run a broker (Ctrl+C to stop)
  create-topic <name> [--partitions <n>]             create a topic (default 1 partition)
  topics                                             list topics
  produce <topic> <value>... [--key <key>]           send records (several values = one batch)
  consume <topic> [--partition <n>] [--from <offset>] [--follow]
                                                     print records (all partitions by default)
  consume <topic> --group <id> [--follow]            read as a member of a consumer group:
                                                     share partitions, continue from commits

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
      group: { type: "string" },
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
      required(args[1], "a value");
      // Every value after the topic is one record; they're sent together in one
      // send(), so each partition receives them as a single batch.
      const recordValues = args.slice(1);
      const key = values.key === undefined ? null : Buffer.from(values.key);
      return withConnection(host, port, async (connection) => {
        const records = recordValues.map((value) => ({ key, value: Buffer.from(value) }));
        const stored = await new Producer(connection).send(topic, records);
        for (const [i, place] of stored.entries()) {
          console.log(`"${recordValues[i]}" stored in ${topic} partition ${place.partition} at offset ${place.offset}`);
        }
      });
    }
    case "consume": {
      const topic = required(args[0], "a topic name");
      const group = values.group;
      if (group !== undefined) {
        return withConnection(host, port, (connection) => consumeAsGroup(connection, topic, group, values.follow));
      }
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
  // Outside data/topics/, so the offsets log is never mistaken for a topic.
  const offsets = await OffsetStore.open(join(dataDir, "internal", "offsets"));
  if (offsets.recoveredTail !== null) {
    console.log(`recovered the committed-offsets log: discarded ${offsets.recoveredTail.discardedBytes} bytes of a torn write`);
  }
  const groups = new GroupCoordinator(topics, offsets);
  const log = (line: string): void => console.error(`${new Date().toISOString()} ${line}`);
  const server = await BrokerServer.start(topics, groups, { host, port, log });
  console.log(`EventRail broker listening on ${server.host}:${server.port}, data in ${dataDir}`);

  await new Promise<void>((resolve) => {
    process.once("SIGINT", () => resolve());
    process.once("SIGTERM", () => resolve());
  });
  console.log("shutting down: answering requests in progress, then closing storage");
  await server.close();
  await offsets.close();
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
        printRecord(topic, consumer.partition, record);
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

// Reads as one member of `group`, printing the assignment whenever it changes and
// committing after every poll. Ctrl+C leaves the group cleanly, so the other
// members take over its partitions at once instead of after a timeout.
async function consumeAsGroup(connection: Connection, topic: string, group: string, follow: boolean): Promise<void> {
  const consumer = new GroupConsumer(connection, group, topic);
  let stopping = false;
  const stop = (): void => {
    stopping = true;
  };
  process.once("SIGINT", stop);
  let shownGeneration = 0;
  try {
    while (!stopping) {
      const records = await consumer.poll();
      if (consumer.generation !== shownGeneration) {
        shownGeneration = consumer.generation;
        const owned = consumer.partitions.length === 0 ? "none (more members than partitions)" : consumer.partitions.join(", ");
        console.log(`${consumer.memberId} of group ${group}, generation ${shownGeneration}: partitions ${owned}`);
      }
      for (const record of records) {
        printRecord(topic, record.partition, record);
      }
      await consumer.commit();
      if (records.length === 0) {
        if (!follow) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, FOLLOW_INTERVAL_MS));
      }
    }
  } finally {
    process.off("SIGINT", stop);
    await consumer.close();
  }
}

function printRecord(topic: string, partition: number, record: LogRecord): void {
  const key = record.key === null ? "(none)" : record.key.toString();
  console.log(`${topic}/${partition} offset ${record.offset}  key=${key}  value=${record.value.toString()}`);
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
