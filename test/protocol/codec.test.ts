import { describe, test } from "node:test";
import assert from "node:assert/strict";

import {
  FETCH_RESPONSE_BASE_BYTES,
  MAX_ERROR_MESSAGE_LENGTH,
  decodeHeader,
  decodeRequest,
  decodeResponse,
  encodeRequest,
  encodeResponse,
  fetchRecordBytes,
} from "../../src/protocol/codec.js";
import {
  API_KEYS,
  API_VERSION,
  type ApiName,
  type ApiRequest,
  type ErrorResponse,
  type MessageHeader,
  type SuccessResponse,
} from "../../src/protocol/messages.js";
import { ProtocolError, type ProtocolErrorCode } from "../../src/protocol/protocol-error.js";

// Accepts only a ProtocolError with the given code (used with assert.throws).
function isProtocolError(code: ProtocolErrorCode): (err: unknown) => boolean {
  return (err: unknown) => {
    assert.ok(err instanceof ProtocolError, "expected a ProtocolError");
    assert.equal(err.code, code);
    return true;
  };
}

// The header a client sends for `api` at the current version.
function headerFor(api: ApiName, correlationId = 7): MessageHeader {
  return { apiKey: API_KEYS[api], apiVersion: API_VERSION, correlationId };
}

// A header built by hand, so a test can send any apiKey and version at all.
function headerBytes(apiKey: number, apiVersion: number, correlationId: number): Buffer {
  const bytes = Buffer.alloc(8);
  bytes.writeUInt16BE(apiKey, 0);
  bytes.writeUInt16BE(apiVersion, 2);
  bytes.writeUInt32BE(correlationId, 4);
  return bytes;
}

// One request of every type, with awkward values: a null key, an empty value,
// the largest offset.
const REQUESTS: ApiRequest[] = [
  { type: "createTopic", topic: "orders", partitionCount: 3 },
  { type: "metadata" },
  {
    type: "produce",
    topic: "orders",
    partition: 2,
    records: [
      { key: Buffer.from("user-42"), value: Buffer.from("order placed") },
      { key: null, value: Buffer.alloc(0) },
    ],
  },
  { type: "fetch", topic: "orders", partition: 2, offset: Number.MAX_SAFE_INTEGER, maxRecords: 500 },
];

// One success response of every type, again with awkward values (a negative
// timestamp exercises the signed i64 field).
const RESPONSES: SuccessResponse[] = [
  { type: "createTopic" },
  {
    type: "metadata",
    topics: [
      { name: "orders", partitionCount: 3 },
      { name: "payments", partitionCount: 1 },
    ],
  },
  { type: "produce", baseOffset: 0, lastOffset: Number.MAX_SAFE_INTEGER },
  {
    type: "fetch",
    logEndOffset: 12,
    records: [
      { offset: 10, timestamp: 1_700_000_000_000, key: Buffer.from("user-42"), value: Buffer.from("order placed") },
      { offset: 11, timestamp: -1, key: null, value: Buffer.alloc(0) },
    ],
  },
];

describe("requests", () => {
  test("every request type round-trips", () => {
    for (const request of REQUESTS) {
      assert.deepEqual(decodeRequest(encodeRequest(7, request)), request);
    }
  });

  test("the header carries the API, the version and the correlation ID", () => {
    for (const request of REQUESTS) {
      // 4,294,967,295 is the largest u32, the largest correlation ID there can be.
      const header = decodeHeader(encodeRequest(4_294_967_295, request));
      assert.deepEqual(header, headerFor(request.type, 4_294_967_295));
    }
  });

  test("the documented example: createTopic \"orders\" with 3 partitions", () => {
    const bytes = encodeRequest(7, { type: "createTopic", topic: "orders", partitionCount: 3 });
    assert.deepEqual(
      bytes,
      Buffer.from([
        0x00, 0x00, // apiKey 0 (createTopic)
        0x00, 0x00, // apiVersion 0
        0x00, 0x00, 0x00, 0x07, // correlationId 7
        0x00, 0x06, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x73, // topic: 6 bytes, "orders"
        0x00, 0x00, 0x00, 0x03, // partitionCount 3
      ]),
    );
  });

  test("a produce request's layout: the record count, then each record's key and value", () => {
    const bytes = encodeRequest(1, {
      type: "produce",
      topic: "t",
      partition: 0,
      records: [{ key: null, value: Buffer.from("hi") }],
    });
    assert.deepEqual(
      bytes,
      Buffer.from([
        0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // header: apiKey 2 (produce), version 0, correlationId 1
        0x00, 0x01, 0x74, // topic: 1 byte, "t"
        0x00, 0x00, 0x00, 0x00, // partition 0
        0x00, 0x00, 0x00, 0x01, // 1 record
        0xff, 0xff, 0xff, 0xff, // key: -1, null
        0x00, 0x00, 0x00, 0x02, 0x68, 0x69, // value: 2 bytes, "hi"
      ]),
    );
  });

  test("a request cut short at any byte is malformed", () => {
    for (const request of REQUESTS) {
      const bytes = encodeRequest(7, request);
      // Every possible shorter length, from 0 bytes to one byte short.
      for (let length = 0; length < bytes.length; length++) {
        assert.throws(
          () => decodeRequest(bytes.subarray(0, length)),
          isProtocolError("MALFORMED_MESSAGE"),
          `${request.type} cut to ${length} bytes`,
        );
      }
    }
  });

  test("extra bytes after the last field are malformed", () => {
    for (const request of REQUESTS) {
      const bytes = Buffer.concat([encodeRequest(7, request), Buffer.from([0])]);
      assert.throws(() => decodeRequest(bytes), isProtocolError("MALFORMED_MESSAGE"), request.type);
    }
  });

  test("a payload shorter than a header is malformed", () => {
    assert.throws(() => decodeHeader(Buffer.alloc(7)), isProtocolError("MALFORMED_MESSAGE"));
  });

  test("an unknown API or version is UNSUPPORTED_VERSION, and its header can still be read", () => {
    const unknownApi = headerBytes(99, 0, 7);
    const futureVersion = headerBytes(API_KEYS.metadata, 1, 8);
    assert.throws(() => decodeRequest(unknownApi), isProtocolError("UNSUPPORTED_VERSION"));
    assert.throws(() => decodeRequest(futureVersion), isProtocolError("UNSUPPORTED_VERSION"));
    // The broker still learns who asked, so it can send an error response back.
    assert.deepEqual(decodeHeader(unknownApi), { apiKey: 99, apiVersion: 0, correlationId: 7 });
  });

  test("a record count larger than the records that follow is malformed", () => {
    // Claims 4,294,967,295 records, but none follow. It must fail at once,
    // without reserving space for them.
    const bytes = Buffer.concat([
      headerBytes(API_KEYS.produce, API_VERSION, 7),
      Buffer.from([0x00, 0x01, 0x74]), // topic "t"
      Buffer.from([0x00, 0x00, 0x00, 0x00]), // partition 0
      Buffer.from([0xff, 0xff, 0xff, 0xff]), // record count
    ]);
    assert.throws(() => decodeRequest(bytes), isProtocolError("MALFORMED_MESSAGE"));
  });

  test("the codec checks shape, not meaning", () => {
    // Each of these breaks a rule that TopicManager or the Log enforces later,
    // with its own error code. To the codec they're just well-formed fields.
    const badButWellFormed: ApiRequest[] = [
      { type: "createTopic", topic: "Not A Valid Name!", partitionCount: 0 },
      { type: "produce", topic: "orders", partition: 7, records: [] },
      { type: "fetch", topic: "missing", partition: 0, offset: 0, maxRecords: 0 },
    ];
    for (const request of badButWellFormed) {
      assert.deepEqual(decodeRequest(encodeRequest(7, request)), request);
    }
  });
});

describe("responses", () => {
  test("every response type round-trips", () => {
    for (const response of RESPONSES) {
      assert.deepEqual(decodeResponse(encodeResponse(headerFor(response.type), response)), response);
    }
  });

  test("a response repeats its request's header", () => {
    for (const response of RESPONSES) {
      const header = headerFor(response.type, 123_456);
      assert.deepEqual(decodeHeader(encodeResponse(header, response)), header);
    }
  });

  test("a fetch response's layout: each record's offset, timestamp, key and value", () => {
    const bytes = encodeResponse(headerFor("fetch", 1), {
      type: "fetch",
      logEndOffset: 6,
      records: [{ offset: 5, timestamp: 1000, key: null, value: Buffer.from("hi") }],
    });
    assert.deepEqual(
      bytes,
      Buffer.from([
        0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // header: apiKey 3 (fetch), version 0, correlationId 1
        0x00, 0x00, // error number 0: success
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, // logEndOffset 6
        0x00, 0x00, 0x00, 0x01, // 1 record
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // offset 5
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, // timestamp 1000 (0x03e8)
        0xff, 0xff, 0xff, 0xff, // key: -1, null
        0x00, 0x00, 0x00, 0x02, 0x68, 0x69, // value: 2 bytes, "hi"
      ]),
    );
  });

  test("fetch response sizes can be calculated before encoding", () => {
    // The broker uses these to keep a fetch response within one frame, so they
    // must match the real encoded length exactly.
    for (const response of RESPONSES) {
      if (response.type === "fetch") {
        let expected = FETCH_RESPONSE_BASE_BYTES;
        for (const record of response.records) {
          expected += fetchRecordBytes(record);
        }
        assert.equal(encodeResponse(headerFor("fetch"), response).length, expected);
      }
    }
  });

  test("an error response is the header, the error number, then the message", () => {
    const bytes = encodeResponse(headerFor("produce", 1), { type: "error", code: "UNKNOWN_TOPIC", message: "no" });
    assert.deepEqual(
      bytes,
      Buffer.from([
        0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // header: apiKey 2 (produce), version 0, correlationId 1
        0x00, 0x08, // error number 8: UNKNOWN_TOPIC
        0x00, 0x02, 0x6e, 0x6f, // message: 2 bytes, "no"
      ]),
    );
  });

  test("an error response can answer any request, even an unknown API or version", () => {
    const header: MessageHeader = { apiKey: 99, apiVersion: 3, correlationId: 7 };
    const error: ErrorResponse = {
      type: "error",
      code: "UNSUPPORTED_VERSION",
      message: "apiKey 99 version 3 is not supported",
    };
    const bytes = encodeResponse(header, error);
    assert.deepEqual(decodeHeader(bytes), header);
    assert.deepEqual(decodeResponse(bytes), error);
  });

  test("a long error message is cut short, never in the middle of a character", () => {
    // "😀" is 2 JavaScript characters but 1 real character, and 4 bytes in UTF-8.
    for (const character of ["x", "😀"]) {
      const encoded = encodeResponse(headerFor("createTopic"), {
        type: "error",
        code: "INVALID_TOPIC_NAME",
        message: character.repeat(100_000), // far more than a string field can hold
      });
      const decoded = decodeResponse(encoded);
      assert.equal(decoded.type, "error");
      if (decoded.type === "error") {
        assert.equal(decoded.message, character.repeat(MAX_ERROR_MESSAGE_LENGTH) + "...");
      }
    }
    // A message of exactly the limit is kept as it is.
    const exact = "x".repeat(MAX_ERROR_MESSAGE_LENGTH);
    const kept = decodeResponse(encodeResponse(headerFor("createTopic"), { type: "error", code: "TOPIC_EXISTS", message: exact }));
    assert.deepEqual(kept, { type: "error", code: "TOPIC_EXISTS", message: exact });
  });

  test("an error number this version doesn't know decodes as UNKNOWN_SERVER_ERROR", () => {
    // As if from a newer broker: error number 999 (0x03e7), then the message "new".
    const bytes = Buffer.concat([
      headerBytes(API_KEYS.fetch, API_VERSION, 7),
      Buffer.from([0x03, 0xe7, 0x00, 0x03, 0x6e, 0x65, 0x77]),
    ]);
    assert.deepEqual(decodeResponse(bytes), { type: "error", code: "UNKNOWN_SERVER_ERROR", message: "new" });
  });

  test("a success response must answer a request of its own API and version", () => {
    const produced: SuccessResponse = { type: "produce", baseOffset: 0, lastOffset: 0 };
    // A broker bug, so RangeError, not ProtocolError.
    assert.throws(() => encodeResponse(headerFor("fetch"), produced), RangeError);
    assert.throws(() => encodeResponse({ ...headerFor("produce"), apiVersion: 1 }, produced), RangeError);
  });

  test("a success response for an unknown API or version is UNSUPPORTED_VERSION", () => {
    // Header, then error number 0: a success, but of an API this client can't read.
    const unknownApi = Buffer.concat([headerBytes(99, 0, 7), Buffer.from([0x00, 0x00])]);
    const futureVersion = Buffer.concat([headerBytes(API_KEYS.createTopic, 1, 7), Buffer.from([0x00, 0x00])]);
    assert.throws(() => decodeResponse(unknownApi), isProtocolError("UNSUPPORTED_VERSION"));
    assert.throws(() => decodeResponse(futureVersion), isProtocolError("UNSUPPORTED_VERSION"));
  });

  test("a response cut short at any byte, or with extra bytes, is malformed", () => {
    for (const response of RESPONSES) {
      const bytes = encodeResponse(headerFor(response.type), response);
      for (let length = 0; length < bytes.length; length++) {
        assert.throws(
          () => decodeResponse(bytes.subarray(0, length)),
          isProtocolError("MALFORMED_MESSAGE"),
          `${response.type} cut to ${length} bytes`,
        );
      }
      const extra = Buffer.concat([bytes, Buffer.from([0])]);
      assert.throws(() => decodeResponse(extra), isProtocolError("MALFORMED_MESSAGE"), response.type);
    }
  });
});
