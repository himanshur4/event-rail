# EventRail

EventRail is a distributed, persistent event streaming broker written in
TypeScript on Node.js. Producers append records to partitioned, append-only
logs; consumers read those logs by offset, coordinate through consumer groups,
and commit their progress. Partitions are replicated across brokers with
leader-follower replication so that acknowledged records survive the loss of
a broker.

> **Status:** early development. The storage layer (log contract, in-memory
> log, segmented file-backed log with an offset index, binary record batch
> format) is implemented and tested;
> crash recovery, networking, consumer groups, and replication are designed but
> not yet built. EventRail uses its own wire
> protocol, is not production-ready, and has not been benchmarked. Sections
> marked *(planned)* describe design targets, not current behavior.

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
  fail. Committed offsets are stored in an internal, replicated topic.
- **Durability on disk.** Each partition log is stored as segment files of
  CRC-checked record batches plus an offset index. On restart, the broker
  scans the newest segment and truncates any partially written batch.

## Architecture

### Layers

Each layer depends only on the layers below it, so storage can be tested
without a network and the protocol can be tested without sockets.

```
 client    producer, consumer, admin                     (planned)
 ─────────────── TCP, length-prefixed frames ───────────────────────
 network   server, connections, request codec            (planned)
 ───────────────────────────────────────────────────────────────────
 broker    request routing, replication,                 (planned)
           consumer groups, cluster metadata
 ───────────────────────────────────────────────────────────────────
 storage   Log contract, MemoryLog, FileLog, segments,   (implemented)
           offset index, record batch format
           crash recovery                                (planned)
```

### Cluster *(planned)*

```
 Producers ──┐                                         ┌── Consumer group "billing"
             ▼                                         ▼
  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │ Broker 1         │  │ Broker 2         │  │ Broker 3         │
  │ (controller)     │  │                  │  │                  │
  │ orders-0 leader  │  │ orders-0 follower│  │ orders-0 follower│
  │ orders-1 follower│  │ orders-1 leader  │  │ orders-1 follower│
  │ orders-2 follower│  │ orders-2 follower│  │ orders-2 leader  │
  └──────────────────┘  └──────────────────┘  └──────────────────┘
     Replication factor 3: each partition has one leader and two followers.
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
orders-0/
  00000000000000000000.log    sealed segment: batches for offsets 0..999
  00000000000000000000.index  one 8-byte entry per batch: relative offset, byte position
  00000000000000001000.log    active segment: new batches are appended here
  00000000000000001000.index
```

- **Write path.** Appends run one at a time through an in-process queue. The
  batch is encoded; if it would push a non-empty active segment past
  `segmentMaxBytes` (default 16 MiB), a new segment is started at the batch's
  offset. The batch is written to the `.log` and fsynced, then its index entry
  is written. In-memory state is updated only after both writes succeed, so
  reads never observe data that is not on disk.
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

Durability and failure assumptions:

| Situation | Behaviour |
|---|---|
| `syncOnAppend: true` (default) | An append resolves only after fsync, so acknowledged records survive power loss |
| `syncOnAppend: false` | Records survive a process crash but may be lost on power loss or OS crash |
| Write or fsync fails | The log closes itself and rejects further use; fsync is never retried |
| A `.log` ends in a partial batch, or its last batch is damaged | `open` rejects with `CORRUPT_LOG` (repair is planned as crash recovery) |
| A batch earlier in a sealed segment is damaged | Not checked at startup; the read that reaches it rejects with `CORRUPT_LOG` |
| Index damaged or missing | Rebuilt from the `.log` on open |
| A segment file is missing from the middle or start | `open` rejects with `CORRUPT_LOG` |
| Power loss right after a new segment file is created | The directory entry is not fsynced, so on some file systems the new file may not survive |
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

## Code layout

```
src/
  common/types.ts          record types shared across layers
  storage/log.ts           Log interface and LogError
  storage/memory-log.ts    in-memory Log implementation
  storage/file-log.ts      file-backed Log: segment list, rolling, append queue
  storage/segment.ts       one .log/.index pair: append, read, index load/repair
  storage/record-batch.ts  binary record batch encoding and decoding
test/
  helpers/temp-dir.ts      per-test temporary directories
  storage/log-contract.ts  contract tests every Log implementation must pass
  storage/memory-log.test.ts
  storage/file-log.test.ts contract, persistence, segment, and index-repair tests
  storage/record-batch.test.ts
```

## Testing approach

- **Contract tests.** The rules of the `Log` interface are written once in
  [test/storage/log-contract.ts](test/storage/log-contract.ts) and run against
  every implementation, so `MemoryLog` and `FileLog` are held to identical
  rules, including concurrent appends issued without awaiting each other.
- **Persistence tests.** `FileLog` is closed and reopened to verify records and
  offsets survive, and its file is compared byte for byte with the expected
  encoded batches. Partially written, damaged, and non-contiguous files are
  verified to be refused.
- **Segment and index tests.** Rolling, oversized batches, exact index bytes,
  missing segment files, and five kinds of index damage (missing, truncated,
  partial entry, non-increasing, out of range), each of which must be repaired
  back to the original index.
- **Failure cases are first-class.** Invalid arguments, empty batches, and
  out-of-range reads are tested alongside the normal path.
- **Exhaustive damage tests.** The record batch decoder is checked against a
  batch truncated at every possible byte (always `incomplete`) and a batch
  with each single byte damaged (never accepted).
- **Planned fault-injection tests** will cover truncated segment files, killed
  leaders, lagging followers, and stale consumer group members.

## Roadmap

- [x] Toolchain: strict TypeScript, build, test runner
- [x] `Log` contract and in-memory log
- [x] Binary record batch format with CRC32 checksums
- [x] File-backed log: single file, serialized appends, fsync per append
- [x] Segmented log files with a persistent offset index
- [ ] Crash recovery: torn-write detection and truncation
- [ ] Topics and partitions with key-based routing
- [ ] Wire protocol, TCP server, producer and consumer clients
- [ ] Consumer groups and committed offsets
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
- **Non-goals:** consensus-based controller election, exactly-once semantics
  and transactions, log compaction, zero-copy transfer, compatibility with
  existing broker wire protocols, compression, and authentication/TLS.
