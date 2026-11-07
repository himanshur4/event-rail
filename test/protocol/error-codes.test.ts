import { describe, test } from "node:test";
import assert from "node:assert/strict";

import { NO_ERROR, errorCodeFromWire, errorCodeToWire, type ErrorCode } from "../../src/protocol/error-codes.js";

// A second copy of the table: the numbers are a contract with every client, so
// one can only change if someone edits both copies on purpose. A new error code
// doesn't compile until it's listed here too.
const EXPECTED_NUMBERS: Record<ErrorCode, number> = {
  UNKNOWN_SERVER_ERROR: 1,
  FRAME_TOO_LARGE: 2,
  MALFORMED_MESSAGE: 3,
  UNSUPPORTED_VERSION: 4,
  INVALID_TOPIC_NAME: 5,
  INVALID_PARTITION_COUNT: 6,
  TOPIC_EXISTS: 7,
  UNKNOWN_TOPIC: 8,
  UNKNOWN_PARTITION: 9,
  CORRUPT_TOPIC: 10,
  MANAGER_CLOSED: 11,
  EMPTY_BATCH: 12,
  INVALID_OFFSET: 13,
  OFFSET_OUT_OF_RANGE: 14,
  INVALID_MAX_RECORDS: 15,
  LOG_CLOSED: 16,
  CORRUPT_LOG: 17,
  RECORD_TOO_LARGE: 18,
};

describe("error codes", () => {
  test("every error code has its fixed number, and the number decodes back to it", () => {
    for (const [code, wireNumber] of Object.entries(EXPECTED_NUMBERS) as [ErrorCode, number][]) {
      assert.equal(errorCodeToWire(code), wireNumber, code);
      assert.equal(errorCodeFromWire(wireNumber), code, code);
    }
  });

  test("the numbers are all different, none is NO_ERROR, and all fit in a u16", () => {
    // The previous test proved the real table equals this copy, so checking the
    // copy here checks the real table too.
    const numbers = Object.values(EXPECTED_NUMBERS);
    assert.equal(new Set(numbers).size, numbers.length);
    assert.equal(NO_ERROR, 0);
    for (const wireNumber of numbers) {
      assert.ok(wireNumber > NO_ERROR && wireNumber <= 0xffff, `${wireNumber}`);
    }
  });

  test("a number this version doesn't know decodes as UNKNOWN_SERVER_ERROR", () => {
    // As if sent by a newer broker that has added more errors.
    assert.equal(errorCodeFromWire(9_999), "UNKNOWN_SERVER_ERROR");
  });
});
