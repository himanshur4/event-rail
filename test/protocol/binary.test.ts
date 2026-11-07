import { describe, test } from "node:test";
import assert from "node:assert/strict";

import { BinaryReader, BinaryWriter } from "../../src/protocol/binary.js";
import { ProtocolError } from "../../src/protocol/protocol-error.js";

// Accepts only a ProtocolError with code MALFORMED_MESSAGE.
function isMalformed(err: unknown): boolean {
  assert.ok(err instanceof ProtocolError, "expected a ProtocolError");
  assert.equal(err.code, "MALFORMED_MESSAGE");
  return true;
}

// Runs `write` on a fresh writer and returns the bytes it produced.
function bytesOf(write: (writer: BinaryWriter) => void): Buffer {
  const writer = new BinaryWriter();
  write(writer);
  return writer.toBuffer();
}

describe("BinaryWriter and BinaryReader", () => {
  test("every field type round-trips, including its extreme values", () => {
    const bytes = bytesOf((w) => {
      w.u8(255);
      w.u16(65_535);
      w.u32(4_294_967_295);
      w.i32(-2_147_483_648);
      w.i32(2_147_483_647);
      w.u64(Number.MAX_SAFE_INTEGER);
      w.i64(Number.MIN_SAFE_INTEGER);
      w.i64(Number.MAX_SAFE_INTEGER);
      w.string("orders");
      w.bytes(Buffer.from([1, 2, 3]));
      w.nullableBytes(Buffer.from("k"));
      w.nullableBytes(null);
    });

    const r = new BinaryReader(bytes);
    assert.equal(r.u8(), 255);
    assert.equal(r.u16(), 65_535);
    assert.equal(r.u32(), 4_294_967_295);
    assert.equal(r.i32(), -2_147_483_648);
    assert.equal(r.i32(), 2_147_483_647);
    assert.equal(r.u64(), Number.MAX_SAFE_INTEGER);
    assert.equal(r.i64(), Number.MIN_SAFE_INTEGER);
    assert.equal(r.i64(), Number.MAX_SAFE_INTEGER);
    assert.equal(r.string(), "orders");
    assert.deepEqual(r.bytes(), Buffer.from([1, 2, 3]));
    assert.deepEqual(r.nullableBytes(), Buffer.from("k"));
    assert.equal(r.nullableBytes(), null);
    r.end(); // nothing left over
  });

  test("numbers are written big-endian", () => {
    assert.deepEqual(
      bytesOf((w) => w.u16(0x0102)),
      Buffer.from([0x01, 0x02]),
    );
    assert.deepEqual(
      bytesOf((w) => w.u32(0x01020304)),
      Buffer.from([0x01, 0x02, 0x03, 0x04]),
    );
    assert.deepEqual(
      bytesOf((w) => w.i32(-1)),
      Buffer.from([0xff, 0xff, 0xff, 0xff]),
    );
    assert.deepEqual(
      bytesOf((w) => w.u64(0x0102)),
      Buffer.from([0, 0, 0, 0, 0, 0, 0x01, 0x02]),
    );
    // Negative numbers use two's complement, as in i32: -2 is all ones except the last bit.
    assert.deepEqual(
      bytesOf((w) => w.i64(-2)),
      Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]),
    );
  });

  test("the documented example: u16 300, then the string \"hi\"", () => {
    const bytes = bytesOf((w) => {
      w.u16(300);
      w.string("hi");
    });
    assert.deepEqual(bytes, Buffer.from([0x01, 0x2c, 0x00, 0x02, 0x68, 0x69]));
  });

  test("string lengths count UTF-8 bytes, not characters", () => {
    // "c", "a", "f" take 1 byte each, "é" takes 2 and "😀" takes 4: 9 bytes.
    const bytes = bytesOf((w) => w.string("café😀"));
    assert.equal(bytes.readUInt16BE(0), 9);
    assert.equal(new BinaryReader(bytes).string(), "café😀");
  });

  test("empty strings and empty bytes round-trip", () => {
    const r = new BinaryReader(
      bytesOf((w) => {
        w.string("");
        w.bytes(Buffer.alloc(0));
        w.nullableBytes(Buffer.alloc(0));
      }),
    );
    assert.equal(r.string(), "");
    assert.deepEqual(r.bytes(), Buffer.alloc(0));
    assert.deepEqual(r.nullableBytes(), Buffer.alloc(0)); // empty, but not null
    r.end();
  });

  test("null is written as length -1 with no data", () => {
    assert.deepEqual(
      bytesOf((w) => w.nullableBytes(null)),
      Buffer.from([0xff, 0xff, 0xff, 0xff]),
    );
  });

  test("written bytes are copied, so later changes to the source don't alter the message", () => {
    const source = Buffer.from("abc");
    const writer = new BinaryWriter();
    writer.bytes(source);
    source.fill(0);
    assert.deepEqual(new BinaryReader(writer.toBuffer()).bytes(), Buffer.from("abc"));
  });

  test("read bytes are copies, independent of the message buffer", () => {
    const message = bytesOf((w) => w.bytes(Buffer.from("abc")));
    const value = new BinaryReader(message).bytes();
    message.fill(0);
    assert.deepEqual(value, Buffer.from("abc"));
  });
});

describe("BinaryReader rejects malformed input", () => {
  test("reading past the end", () => {
    assert.throws(() => new BinaryReader(Buffer.alloc(0)).u8(), isMalformed);
    assert.throws(() => new BinaryReader(Buffer.alloc(3)).u32(), isMalformed);
    assert.throws(() => new BinaryReader(Buffer.alloc(7)).u64(), isMalformed);
    assert.throws(() => new BinaryReader(Buffer.alloc(7)).i64(), isMalformed);
  });

  test("a string or bytes length longer than the data that follows", () => {
    // Each claims 10 bytes but only 2 follow.
    assert.throws(() => new BinaryReader(Buffer.from([0x00, 0x0a, 0x68, 0x69])).string(), isMalformed);
    assert.throws(() => new BinaryReader(Buffer.from([0x00, 0x00, 0x00, 0x0a, 1, 2])).bytes(), isMalformed);
  });

  test("a huge claimed length is rejected without reserving memory for it", () => {
    // Claims 4 GiB with nothing after it. If the reader reserved memory before
    // checking, this would fail with an out-of-memory error instead.
    assert.throws(() => new BinaryReader(Buffer.from([0xff, 0xff, 0xff, 0xff])).bytes(), isMalformed);
  });

  test("invalid UTF-8 in a string", () => {
    // 0xc3 starts a 2-byte character, but 0x28 can't be its second byte.
    assert.throws(() => new BinaryReader(Buffer.from([0x00, 0x02, 0xc3, 0x28])).string(), isMalformed);
  });

  test("a negative byte length other than -1", () => {
    // 0xfffffffe is -2 as a signed 32-bit number.
    assert.throws(() => new BinaryReader(Buffer.from([0xff, 0xff, 0xff, 0xfe])).nullableBytes(), isMalformed);
  });

  test("64-bit values outside the safe integer range", () => {
    assert.throws(() => new BinaryReader(Buffer.alloc(8, 0xff)).u64(), isMalformed);
    // 0x80 then seven 0x00 is the most negative i64, -2^63; 0x7f then 0xff... the most positive.
    assert.throws(() => new BinaryReader(Buffer.from([0x80, 0, 0, 0, 0, 0, 0, 0])).i64(), isMalformed);
    assert.throws(() => new BinaryReader(Buffer.from([0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])).i64(), isMalformed);
  });

  test("leftover bytes after the last field", () => {
    const reader = new BinaryReader(Buffer.from([1, 2]));
    reader.u8();
    assert.throws(() => reader.end(), isMalformed);
  });

  test("the error names the field and where reading stopped", () => {
    const reader = new BinaryReader(Buffer.from([0, 1, 2]));
    reader.u16();
    assert.throws(
      () => reader.u32(),
      (err: unknown) => {
        assert.ok(err instanceof ProtocolError);
        assert.match(err.message, /u32 needs 4 bytes at position 2, but only 1 remain/);
        return true;
      },
    );
  });
});

describe("BinaryWriter rejects out-of-range values", () => {
  test("numbers outside their field's range, or not whole numbers", () => {
    const w = new BinaryWriter();
    const badWrites = [
      () => w.u8(256),
      () => w.u8(-1),
      () => w.u8(1.5),
      () => w.u16(65_536),
      () => w.u32(2 ** 32),
      () => w.u32(Number.NaN),
      () => w.i32(2 ** 31),
      () => w.i32(-(2 ** 31) - 1),
      () => w.u64(-1),
      () => w.u64(2 ** 53),
      () => w.i64(2 ** 53),
      () => w.i64(-(2 ** 53)),
      () => w.i64(0.5),
    ];
    for (const [index, write] of badWrites.entries()) {
      assert.throws(write, RangeError, `bad write #${index}`);
    }
  });

  test("a string longer than 65,535 bytes", () => {
    assert.throws(() => new BinaryWriter().string("x".repeat(65_536)), RangeError);
    new BinaryWriter().string("x".repeat(65_535)); // exactly the limit is fine
  });
});
