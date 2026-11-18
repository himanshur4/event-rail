# EventRail

EventRail is a persistent event streaming broker written in TypeScript on
Node.js. Producers append records to partitioned, append-only logs on disk;
consumers read those logs by offset, split a topic's partitions through
consumer groups, and commit their progress so that another member can continue
where they stopped.

## Contents

- [How it works](#how-it-works)
- [Architecture](#architecture)
- [What is implemented](#what-is-implemented)
- [Getting started](#getting-started)
- [Code layout](#code-layout)
- [Testing approach](#testing-approach)
- [Roadmap](#roadmap)
- [Design decisions and limitations](#design-decisions-and-limitations)

## How it works

### Core model

The central abstraction is the **log**: an ordered, append-only sequence of
records. Each record receives an **offset**, its zero-based position in the
log. Offsets are contiguous and never reused, so `(log, offset)` identifies a
record permanently.

```
offset:   0      1      2      3      4
        [rec0] [rec1] [rec2] [rec3] [rec4]   ◀── next append receives offset 5
                 ▲                    ▲          (the log end offset)
            consumer A           consumer B
```

Reading does not remove records. Each consumer tracks its own position, so
many consumers can read the same log independently and any of them can replay
from an earlier offset.

### Record lifecycle *(planned)*

```
Producer
  │ 1. choose partition: hash(key) % partitionCount (round-robin if no key)
  ▼
Partition leader (one broker)
  │ 2. append batch to the partition log, assign offsets
  │ 3. followers fetch the new records and append them to their own copies
  │ 4. high watermark advances once all in-sync replicas have the records
  │ 5. acknowledge the producer (according to its acks setting)
  ▼
Consumer group member assigned to that partition
  │ 6. fetch from its committed offset, up to the high watermark
  │ 7. process records
  └ 8. commit the next offset to read
```

- **Partitions.** A topic is split into several independent logs. Records with
  the same key always go to the same partition, so ordering is guaranteed
  per partition (and therefore per key), not across a whole topic.
- **Replication.** Each partition has one leader and several followers on
  different brokers. Followers pull from the leader. The **high watermark** is
  the highest offset copied to every in-sync replica (ISR); consumers never
  read past it, so they never see records that a leader failover could discard.
- **Acknowledgements.** Producers choose `acks=0` (don't wait), `acks=1`
  (leader has written it), or `acks=all` (every in-sync replica has it).
- **Consumer groups.** Members of a group split a topic's partitions between
  them; each partition is read by exactly one member. A coordinator tracks
  membership through heartbeats and reassigns partitions when members join or
  fail. Committed offsets are stored in an internal log on the broker.
- **Durability on disk.** Each partition log is stored as segment files of
  CRC-checked record batches plus an offset index. On restart, the broker
  scans the newest segment and truncates any partially written batch.

## Architecture

### Layers

Each layer depends only on the layers below it, so storage can be tested
without a network and the protocol can be tested without sockets.

```
 client    producer, consumer, admin, CLI                (implemented)
 ─────────────── TCP, length-prefixed frames ───────────────────────
 network   framing, field encoding, message codec        (implemented)
           TCP server, per-connection request queue      (implemented)
 ───────────────────────────────────────────────────────────────────
 broker    request handling (single broker)              (implemented)
           consumer groups, committed offsets            (implemented)
           replication, cluster metadata                 (planned)
 ───────────────────────────────────────────────────────────────────
 topic     TopicManager, partitioner, topic metadata     (implemented)
 ───────────────────────────────────────────────────────────────────
 storage   Log contract, MemoryLog, FileLog, segments,   (implemented)
           offset index, record batch format,
           crash recovery
```


The controller (broker 1) tracks broker liveness, assigns partition replicas,
and elects a new leader from the in-sync replicas when a leader fails.

## What is implemented

**The `Log` contract** ([src/storage/log.ts](src/storage/log.ts)), the
interface every partition log implementation must satisfy:

| Rule | Behaviour |
|---|---|
| Offsets | Start at 0 and increase by 1 with no gaps |
| Batches | `append` stores a batch entirely or not at all; empty batches are rejected |
| Reading at the end | `read(logEndOffset, n)` returns `[]` (consumer is caught up) |
| Reading past the end | Rejected with `OFFSET_OUT_OF_RANGE` |
| Invalid arguments | Negative or non-integer offsets, and `maxRecords < 1`, are rejected |
| Ordering | Concurrent appends are applied in the order they were called |
| Isolation | Callers cannot modify stored records through buffers they pass in or receive |
| Closing | After `close()`, `append` and `read` reject with `LOG_CLOSED`; closing twice is allowed |

**`MemoryLog`** ([src/storage/memory-log.ts](src/storage/memory-log.ts)), an
array-backed implementation used as the reference for the contract. It does
not persist data.

**Record batch format** ([src/storage/record-batch.ts](src/storage/record-batch.ts)),
the binary layout batches are stored in on disk. All integers are big-endian.

```
offset  size  field
 0      4     batchLength   total batch size in bytes (length prefix)
 4      4     crc32         checksum of bytes 8..end
 8      8     baseOffset    offset of the first record
16      8     timestamp     append time, Unix epoch milliseconds
24      4     recordCount   number of records that follow
28      ...   records       keyLength (int32, -1 = null), key,
                            valueLength (uint32), value
```

Decoding never throws on bad data. It returns `ok` with the batch and its
size, `incomplete` when the buffer ends before the batch does (for example, a
write cut short by a crash), or `corrupt` when the checksum fails or a length
or count is inconsistent. The checksum covers the base offset as well as the
records, and every length is bounds-checked even when the checksum passes.

**`FileLog`** ([src/storage/file-log.ts](src/storage/file-log.ts)) and
**`Segment`** ([src/storage/segment.ts](src/storage/segment.ts)), a log stored
as a series of segment files in one directory. It passes the same contract
suite as `MemoryLog`, both with default and with deliberately tiny segments.

```
topics/orders/0/              one partition's log directory
  00000000000000000000.log    sealed segment: batches for offsets 0..999
  00000000000000000000.index  one 8-byte entry per batch: relative offset, byte position
  00000000000000001000.log    active segment: new batches are appended here
  00000000000000001000.index
```

- **Write path.** Appends run one at a time through an in-process queue. The
  batch is encoded; if it would push a non-empty active segment past
  `segmentMaxBytes` (default 16 MiB), the active segment is flushed and sealed,
  and a new segment is started at the batch's offset. The batch is written to
  the `.log` and fsynced, then its index entry is written. In-memory state is
  updated only after both writes succeed, so reads never observe data that is
  not on disk.
- **The index only lists durable batches.** An index entry is written only
  after an fsync of its batch has completed. With `syncOnAppend: false`,
  entries wait in memory and are written when the segment is flushed: when it
  is sealed, or when the log is closed cleanly. Crash recovery relies on this.
- **Read path.** Binary search finds the segment containing the requested
  offset, then the batch within it; only the needed batches are read and
  decoded, continuing into later segments as required.
- **Startup.** Each segment loads its index instead of scanning its `.log`.
  The last indexed batch and anything after it are re-decoded to confirm the
  index and to add entries the index is missing. Segments must be contiguous
  and the first must start at offset 0.
- **The `.log` is the source of truth.** The index is not fsynced. A missing,
  partially written, non-increasing, or out-of-range index is rebuilt from a
  full scan of its `.log`.

**Crash recovery.** A crash during an append can leave a torn write at the end
of the active segment: part of a batch, or reserved bytes that never received
their data. `FileLog.open` cuts the `.log` back to the end of its last valid
batch, fsyncs the cut, rebuilds the index, and reports what it removed in
`recoveredTail`. Cutting is safe because an append is acknowledged only after
its batch is fsynced, so a torn batch was never acknowledged.

Damage that a crash cannot explain is refused with `CORRUPT_LOG` and left
untouched. The invalid data is cut off only when all three rules hold:

1. It is in the active segment. Sealed segments are flushed before the log
   moves past them, so a crash cannot tear them.
2. It is incomplete or fails its checksum. A complete, valid batch with an
   unexpected offset is not something a crash produces.
3. The index lists no batch at or after the cut. Because the index only lists
   durable batches, such an entry proves the data there was acknowledged.

**Topics and partitions** ([src/topic/](src/topic/)). A topic is a named set
of partitions, and each partition is an independent `FileLog` with its own
offsets, append queue, and crash recovery. `TopicManager` creates topics,
opens all of them at startup, and returns the `Log` for a given
`(topic, partition)`.

```
<dataDir>/topics/
  orders/
    topic.json   {"version":1,"name":"orders","partitionCount":3}
    0/  1/  2/   one FileLog directory per partition
```

- **Key-based routing.** `Partitioner` maps a record with a key to
  `murmur3(key) % partitionCount`, so every record with the same key goes to
  the same partition and stays in order. Records without a key are spread
  round-robin. The hash is MurmurHash3 (x86_32, seed 0), pinned by tests to its
  published values, because changing it would move existing keys to different
  partitions. Ordering is guaranteed per partition, not across a topic.
- **Creation has a commit point.** Partition directories are created first, and
  `topic.json` is written last, atomically (temporary file, fsync, rename). A
  directory without `topic.json` is a creation that never finished: it is
  ignored at startup and replaced if the topic is created again.
- **Names are safe directory names.** Lowercase letters, digits, `.`, `_`, and
  `-`, starting with a letter or digit, at most 100 characters, not ending in
  `.`, and not a Windows device name (`con`, `nul`, `com1`, ...). Lowercase
  only, because case-insensitive file systems would map `Orders` and `orders`
  to the same directory.
- **Startup checks.** A `topic.json` that is invalid, has an unknown version,
  or names a different topic, and a missing partition directory, are refused
  with `CORRUPT_TOPIC` rather than silently replaced by an empty log. Torn
  writes repaired in any partition are reported in `recoveredTails`.
- **Partition counts are fixed** once a topic is created (1 to 1000), since
  changing the count would change which partition each key maps to.

**Wire protocol** ([src/protocol/](src/protocol/)). TCP is a byte stream
without message boundaries, so every message travels as a frame: a 4-byte
big-endian length followed by the payload. `FrameDecoder` rebuilds whole frames
from chunks split at any byte, copying each byte once, and rejects a frame
whose announced length exceeds the limit (8 MiB by default) before accepting
any of its payload. `BinaryWriter` and `BinaryReader` encode the fields
messages are built from: big-endian `u8`, `u16`, `u32`, `i32`, `u64`, `i64`,
length-prefixed UTF-8 strings, and byte fields with `-1` meaning null. Every
read is bounds-checked, invalid UTF-8 and leftover bytes are rejected, and all
malformed input raises `ProtocolError` rather than crashing.

Every payload, request or response, starts with the same 8-byte header:
`apiKey u16 | apiVersion u16 | correlationId u32`. Nine APIs are defined, all
at version 0: `createTopic`, `metadata`, `produce` (records for one partition),
`fetch` (records from one partition, with its log end offset), and the consumer
group APIs `joinGroup`, `heartbeat`, `leaveGroup`, `commitOffset`, and
`fetchOffset`. A response
repeats its request's header, so a client can match it to the request, then
carries an error number: `0` followed by the API's fields, or a nonzero number
followed by a message. Every error code of every layer has a permanent wire
number, and the compiler rejects a code without one. Decoding an unknown API
or version raises `UNSUPPORTED_VERSION` instead of misreading the body, and an
error response can be encoded for any header. The codec checks only a message's
shape; topic and log rules stay in `TopicManager` and the log.

**Broker server** ([src/server/](src/server/), [src/broker/](src/broker/)).
`BrokerServer` listens on TCP (127.0.0.1:7400 by default) and gives each
connection its own `FrameDecoder` and request queue. Requests on one connection
are carried out one at a time, in arrival order, so a producer's batches are
stored in the order it sent them; separate connections are served concurrently.
`RequestHandler` maps each request onto `TopicManager`, the partition logs, and
the group coordinator, so a produce response is sent only after the append has completed (and been fsynced
by default). The broker enforces two size rules: a record's key and value may
total at most 1 MiB, and a fetch response holds only as many records as fit in
one frame, but always at least one, so every stored record can be fetched and a
consumer always makes progress. A frame that is too large or too short for a
header closes that one connection; any other failure is answered with an error
response and the connection stays open. Unexpected errors are reported as
`UNKNOWN_SERVER_ERROR` without internal details. Each connection pauses reading
while 32 requests are waiting and while the client is not reading its answers;
idle connections close after 10 minutes, and at most 1000 connections are
accepted. `close()` stops accepting connections, answers every request already
received, then closes the connections; storage is closed after it.

**Clients** ([src/client/](src/client/)). `Connection` sends requests with
increasing correlation IDs and resolves each one when the answer with its ID
arrives, so many requests can be in flight on one connection. Each request times
out after 30 seconds; a late answer is discarded rather than given to another
request. An error response rejects with `BrokerError` (the request was not
carried out); a closed connection, a timeout, or an invalid answer rejects with
`ConnectionError` (the request may or may not have been carried out). `Admin`
creates and lists topics. `Producer` fetches a topic's partition count once,
chooses each record's partition with the partitioner, sends one produce request
per partition, and returns each record's partition and offset. `Consumer` reads
one partition from a position it keeps in memory and reports its lag.

**Consumer groups** ([src/group/](src/group/),
[src/client/group-consumer.ts](src/client/group-consumer.ts)). The members of a
group split one topic's partitions between them, so each partition is read by
one member at a time. The broker's `GroupCoordinator` keeps the membership in
memory, and the server checks its timeouts every 100 ms.

- **Rebalancing.** A member that joins, leaves, or goes silent starts a
  rebalance: every member must join again, and joins are answered "waiting"
  until all members have rejoined or the rebalance timeout (10 s) removes those
  that did not. The partitions are then assigned round-robin in join order, and
  the group's generation number increases by one.
- **Liveness.** Members send heartbeats; one that is silent for the session
  timeout (10 s) is removed. A heartbeat during a rebalance is answered
  `REBALANCE_IN_PROGRESS`, which is how members learn they must join again.
- **Fencing.** Heartbeats and commits carry the member's generation. An older
  generation is refused with `STALE_GENERATION`, and a commit for a partition
  the member does not own with `NOT_PARTITION_OWNER`, so a member that missed a
  rebalance cannot overwrite the progress of the partition's new owner. During a
  rebalance, commits with the current generation are still accepted, so members
  can save their progress before they rejoin.
- **Committed offsets.** `OffsetStore` keeps each group's committed offset per
  topic partition: the next offset the group will read. Every commit is
  appended as one record to an internal `FileLog`, outside `data/topics/`, so it
  is never listed or reachable as a topic, and is acknowledged only after it is
  fsynced. On startup the store replays the log and keeps the latest commit for
  each partition, after the log's crash recovery has cut off any torn commit. A
  record that does not decode as a commit stops the store from opening
  (`CORRUPT_LOG`).
- **`GroupConsumer`** joins, sends heartbeats in the background, and polls its
  partitions from the group's committed offsets (offset 0 if there are none).
  It commits when asked, before it rejoins, and before it leaves. Delivery is at
  least once: records read after the last commit are read again by the
  partition's next owner.

Durability and failure assumptions:

| Situation | Behaviour |
|---|---|
| `syncOnAppend: true` (default) | An append resolves only after fsync, so acknowledged records survive power loss |
| `syncOnAppend: false` | Records survive a process crash but may be lost on power loss or OS crash; data is fsynced when a segment is sealed and on a clean close |
| Write or fsync fails | The log closes itself and rejects further use; fsync is never retried |
| An append fails after its batch reached the disk | The batch is kept on reopen; a producer that retries may store it twice |
| The active segment ends in a torn write | Cut off on open and reported in `recoveredTail` |
| Damage to a batch the index lists, or in a sealed segment | `open` rejects with `CORRUPT_LOG`; files are not modified |
| A batch earlier in a segment is damaged, with valid data indexed after it | Usually not detected at startup (only the last indexed batch is re-checked); never cut off; the read that reaches it rejects with `CORRUPT_LOG` |
| Index damaged or missing | Rebuilt from the `.log` on open |
| A segment file is missing from the middle or start | `open` rejects with `CORRUPT_LOG` |
| A crash while a topic is being created | No `topic.json`, so the directory is not a topic; it is replaced if the topic is created again |
| `topic.json` damaged, or a partition directory missing | `TopicManager.open` rejects with `CORRUPT_TOPIC` |
| Power loss right after a new file or directory is created (segment, topic) | Directory entries are not fsynced, so on some file systems the new entry may not survive |
| A group member stops without leaving | Removed after the session timeout; its partitions are reassigned, and records it read after its last commit are read again |
| The broker restarts | Committed offsets survive; group membership does not, so consumers must join again |
| Two processes open the same directory | Not supported and not yet detected |

## Getting started

Requirements: Node.js 22 or later.

```sh
npm install        # install dev dependencies
npm run typecheck  # type-check only, writes no files
npm run build      # type-check and compile src/ and test/ into dist/
npm test           # build, then run the test suite with node:test
```

The build refuses to emit output when there are type errors
(`noEmitOnError`), so tests never run against code that failed type checking.

Run a broker and talk to it from other terminals:

```sh
node dist/src/cli.js broker --data ./data            # Ctrl+C stops it cleanly
node dist/src/cli.js create-topic orders --partitions 3
node dist/src/cli.js produce orders "order placed" --key user-4
node dist/src/cli.js consume orders                  # add --follow to keep reading
node dist/src/cli.js consume orders --group billing --follow
node dist/src/cli.js topics
```

Several values in one `produce` command are sent as one batch. Run the group
command in two terminals to see the partitions split between them; stopping one
with Ctrl+C hands its partitions to the other.

Every command accepts `--host` and `--port` (default `127.0.0.1:7400`).

## Code layout

```
src/
  common/types.ts          record types shared across layers
  storage/log.ts           Log interface and LogError
  storage/memory-log.ts    in-memory Log implementation
  storage/file-log.ts      file-backed Log: segment list, rolling, append queue
  storage/segment.ts       one .log/.index pair: append, read, index repair, crash recovery
  storage/record-batch.ts  binary record batch encoding and decoding
  topic/partitioner.ts     key hashing (MurmurHash3) and round-robin partition choice
  topic/topic-metadata.ts  topic name rules, TopicError, atomic topic.json
  topic/topic-manager.ts   creates and opens topics; routes to partition logs
  protocol/frame.ts        length-prefixed frames and the streaming FrameDecoder
  protocol/binary.ts       BinaryWriter and BinaryReader for message fields
  protocol/messages.ts     request and response types, API keys, message header
  protocol/codec.ts        encodes and decodes every request and response
  protocol/error-codes.ts  permanent wire numbers for every error code
  protocol/protocol-error.ts  ProtocolError for malformed or unsupported input
  broker/request-handler.ts  carries out requests against TopicManager; size rules
  broker/request-error.ts  RequestError (RECORD_TOO_LARGE)
  server/broker-server.ts  TCP listener, connection limit, graceful shutdown
  server/server-connection.ts  one connection: framing, ordered queue, back-pressure
  client/connection.ts     correlation IDs, pending requests, timeouts
  client/client-errors.ts  BrokerError and ConnectionError
  client/admin.ts          create and list topics
  client/producer.ts       partition choice, one request per partition
  client/consumer.ts       reads one partition from an in-memory position
  common/network-defaults.ts  default host and port
  group/offset-store.ts    committed offsets of consumer groups, in an internal log
  group/group-coordinator.ts  membership, rebalances, generations, partition assignment
  group/group-error.ts     GroupError and its codes
  client/group-consumer.ts joins a group, sends heartbeats, polls and commits its partitions
  cli.ts                   command-line tool: broker, topics, produce, consume
```
## Roadmap

- [x] Toolchain: strict TypeScript, build, test runner
- [x] `Log` contract and in-memory log
- [x] Binary record batch format with CRC32 checksums
- [x] File-backed log: single file, serialized appends, fsync per append
- [x] Segmented log files with a persistent offset index
- [x] Crash recovery: torn-write detection and truncation
- [x] Topics and partitions with key-based routing
- [x] Wire protocol, TCP server, producer and consumer clients
- [x] Consumer groups and committed offsets
- [ ] Multi-broker cluster with a controller
- [ ] Replication: in-sync replicas, high watermark, `acks=0/1/all`
- [ ] Leader failover with leader epochs

## Design decisions and limitations

- **Standard library only.** Storage, networking, and checksums use Node.js
  built-ins (`node:fs`, `node:net`, `node:zlib`). The only dependencies are
  development tools (TypeScript and Node type definitions).
- **Static controller** *(planned)*. Broker 1 is always the controller; there
  is no controller election, so controller failure stops leader elections
  until it restarts.
- **Offsets as JavaScript numbers.** Offsets are exact up to 2^53 − 1, far
  beyond any realistic log length for this project.
- **Partitioning happens in the producer.** The broker's
  storage is addressed by `(topic, partition)`; choosing the partition from the
  key is the producer's job, as in the record lifecycle above.
- **One partition per produce or fetch request.** Each request has a single
  error code rather than one per partition; writing to several partitions
  takes several requests.
- **At-least-once on failure.** A produce that fails with `ConnectionError` may
  already have been stored; retrying it can store the records twice.
- **Consumer group limits.** A group reads one topic; membership is kept in
  memory; every rebalance pauses the whole group until all members have
  rejoined; and the offsets log is not compacted, so it grows with every
  commit. A plain `Consumer` keeps its position in memory only, and fetching is
  polling (no waiting on the broker for new records).
- **No authentication or encryption.** The broker listens on 127.0.0.1 by
  default; exposing it to a network is a deliberate choice.
- **A producer send is one request per partition.** Large sends are not split
  into several requests, so each partition's share must fit in one frame, and a
  send is not atomic across partitions: if one partition's request fails, the
  others may still have been stored.
- **Non-goals:** consensus-based controller election, exactly-once semantics
  and transactions, log compaction, zero-copy transfer, compatibility with
  existing broker wire protocols, compression, and authentication/TLS.
