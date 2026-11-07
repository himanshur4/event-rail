// The field types every protocol message is built from, all numbers big-endian:
//
//   u8, u16, u32    unsigned, 1, 2 and 4 bytes
//   i32             signed, 4 bytes
//   u64             8 bytes, 0 .. 2^53 - 1 (the largest exact JavaScript number)
//   i64             8 bytes, -(2^53 - 1) .. 2^53 - 1
//   string          u16 byte length, then that many bytes of UTF-8 text
//   bytes           u32 byte length, then that many bytes
//   nullableBytes   i32 byte length (-1 means null, no bytes follow), then the bytes
//
// Written values come from our own code, so a value out of range is a bug and
// throws RangeError. Read bytes come from the network and can't be trusted, so
// every read is bounds-checked and a problem throws MALFORMED_MESSAGE.

import { ProtocolError } from "./protocol-error.js";

const MAX_U8 = 0xff;
const MAX_U16 = 0xffff;
const MAX_U32 = 0xffff_ffff;
const MIN_I32 = -0x8000_0000;
const MAX_I32 = 0x7fff_ffff;
const NULL_LENGTH = -1;
const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const MIN_SAFE_BIGINT = BigInt(Number.MIN_SAFE_INTEGER);

// Fatal: invalid bytes must be rejected, not silently replaced by "�", which
// could turn a topic name into a different name.
const strictUtf8 = new TextDecoder("utf-8", { fatal: true });

/**
 * Builds a message field by field. Call the methods in order, then
 * toBuffer() for the bytes. Buffers passed to bytes() are copied at that point.
 */
export class BinaryWriter {
  private readonly parts: Buffer[] = [];

  u8(value: number): void {
    checkInteger(value, 0, MAX_U8, "u8");
    const part = Buffer.alloc(1);
    part.writeUInt8(value, 0);
    this.parts.push(part);
  }

  u16(value: number): void {
    checkInteger(value, 0, MAX_U16, "u16");
    const part = Buffer.alloc(2);
    part.writeUInt16BE(value, 0);
    this.parts.push(part);
  }

  u32(value: number): void {
    checkInteger(value, 0, MAX_U32, "u32");
    const part = Buffer.alloc(4);
    part.writeUInt32BE(value, 0);
    this.parts.push(part);
  }

  i32(value: number): void {
    checkInteger(value, MIN_I32, MAX_I32, "i32");
    const part = Buffer.alloc(4);
    part.writeInt32BE(value, 0);
    this.parts.push(part);
  }

  u64(value: number): void {
    checkInteger(value, 0, Number.MAX_SAFE_INTEGER, "u64");
    const part = Buffer.alloc(8);
    part.writeBigUInt64BE(BigInt(value), 0);
    this.parts.push(part);
  }

  i64(value: number): void {
    checkInteger(value, Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER, "i64");
    const part = Buffer.alloc(8);
    part.writeBigInt64BE(BigInt(value), 0);
    this.parts.push(part);
  }

  /** A UTF-8 string of at most 65,535 bytes, after a u16 length. */
  string(value: string): void {
    // The length counts bytes, not characters: "é" takes 2 bytes, "😀" takes 4.
    const encoded = Buffer.from(value, "utf8");
    if (encoded.length > MAX_U16) {
      throw new RangeError(`string is ${encoded.length} bytes; the limit is ${MAX_U16}`);
    }
    this.u16(encoded.length);
    this.parts.push(encoded);
  }

  /** Raw bytes after a u32 length. */
  bytes(value: Buffer): void {
    this.u32(value.length);
    this.parts.push(Buffer.from(value));
  }

  /** Raw bytes, or null, after an i32 length (-1 for null). */
  nullableBytes(value: Buffer | null): void {
    if (value === null) {
      this.i32(NULL_LENGTH);
      return;
    }
    this.i32(value.length);
    this.parts.push(Buffer.from(value));
  }

  /** All fields written so far, joined into one Buffer. */
  toBuffer(): Buffer {
    return Buffer.concat(this.parts);
  }
}

/**
 * Reads fields from a received message, in the order they were written.
 * Throws ProtocolError MALFORMED_MESSAGE if the bytes don't fit the fields
 * requested. Call end() after the last field to reject leftover bytes.
 */
export class BinaryReader {
  private readonly buffer: Buffer;
  private position = 0;

  constructor(buffer: Buffer) {
    this.buffer = buffer;
  }

  /** Bytes not read yet. */
  get remaining(): number {
    return this.buffer.length - this.position;
  }

  u8(): number {
    return this.take(1, "u8").readUInt8(0);
  }

  u16(): number {
    return this.take(2, "u16").readUInt16BE(0);
  }

  u32(): number {
    return this.take(4, "u32").readUInt32BE(0);
  }

  i32(): number {
    return this.take(4, "i32").readInt32BE(0);
  }

  u64(): number {
    const value = this.take(8, "u64").readBigUInt64BE(0);
    if (value > MAX_SAFE_BIGINT) {
      throw malformed(`u64 value ${value} exceeds the largest safe integer`);
    }
    return Number(value);
  }

  i64(): number {
    const value = this.take(8, "i64").readBigInt64BE(0);
    if (value < MIN_SAFE_BIGINT || value > MAX_SAFE_BIGINT) {
      throw malformed(`i64 value ${value} is outside the safe integer range`);
    }
    return Number(value);
  }

  string(): string {
    const length = this.u16();
    const bytes = this.take(length, "string");
    try {
      return strictUtf8.decode(bytes);
    } catch {
      throw malformed("string is not valid UTF-8");
    }
  }

  bytes(): Buffer {
    const length = this.u32();
    // Copied, so the result doesn't keep the whole message in memory.
    return Buffer.from(this.take(length, "bytes"));
  }

  nullableBytes(): Buffer | null {
    const length = this.i32();
    if (length === NULL_LENGTH) {
      return null;
    }
    if (length < 0) {
      throw malformed(`invalid byte length ${length}`);
    }
    return Buffer.from(this.take(length, "bytes"));
  }

  /** Throws MALFORMED_MESSAGE unless every byte has been read. */
  // Leftover bytes mean sender and receiver disagree about the message layout.
  end(): void {
    if (this.remaining !== 0) {
      throw malformed(`${this.remaining} unexpected bytes after the last field`);
    }
  }

  // Every read goes through here, so a claimed length is checked against the
  // bytes actually present before anything is copied or allocated.
  private take(length: number, field: string): Buffer {
    if (length > this.remaining) {
      throw malformed(
        `${field} needs ${length} bytes at position ${this.position}, but only ${this.remaining} remain`,
      );
    }
    const view = this.buffer.subarray(this.position, this.position + length);
    this.position += length;
    return view;
  }
}

function checkInteger(value: number, min: number, max: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < min || value > max) {
    throw new RangeError(`${field} must be an integer from ${min} to ${max}, got ${value}`);
  }
}

function malformed(detail: string): ProtocolError {
  return new ProtocolError("MALFORMED_MESSAGE", detail);
}
