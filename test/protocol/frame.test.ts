import { describe, test } from "node:test";
import assert from "node:assert/strict";

import { DEFAULT_MAX_FRAME_BYTES, FRAME_HEADER_SIZE, FrameDecoder, encodeFrame } from "../../src/protocol/frame.js";
import { ProtocolError, type ProtocolErrorCode } from "../../src/protocol/protocol-error.js";

// Accepts only a ProtocolError with the given code (used with assert.throws).
function isProtocolError(code: ProtocolErrorCode): (err: unknown) => boolean {
  return (err: unknown) => {
    assert.ok(err instanceof ProtocolError, "expected a ProtocolError");
    assert.equal(err.code, code);
    return true;
  };
}

// Frame payloads as text, so tests can compare them as strings.
function texts(frames: Buffer[]): string[] {
  return frames.map((frame) => frame.toString());
}

// A bare 4-byte length prefix announcing a frame of `length` bytes.
function lengthPrefix(length: number): Buffer {
  const header = Buffer.alloc(FRAME_HEADER_SIZE);
  header.writeUInt32BE(length, 0);
  return header;
}

describe("encodeFrame", () => {
  test("writes a 4-byte big-endian length, then the payload", () => {
    // 0x61 0x62 0x63 are the bytes of "abc".
    assert.deepEqual(encodeFrame(Buffer.from("abc")), Buffer.from([0, 0, 0, 3, 0x61, 0x62, 0x63]));
  });

  test("an empty payload is just a zero length", () => {
    assert.deepEqual(encodeFrame(Buffer.alloc(0)), Buffer.from([0, 0, 0, 0]));
  });

  test("a payload over the limit is rejected; exactly the limit is accepted", () => {
    assert.throws(() => encodeFrame(Buffer.alloc(11), 10), isProtocolError("FRAME_TOO_LARGE"));
    assert.equal(encodeFrame(Buffer.alloc(10), 10).length, 14);
  });
});

describe("FrameDecoder", () => {
  test("decodes a frame that arrives in one chunk", () => {
    const decoder = new FrameDecoder();
    assert.deepEqual(texts(decoder.push(encodeFrame(Buffer.from("hello")))), ["hello"]);
    assert.equal(decoder.bufferedBytes, 0);
  });

  test("decodes several frames that arrive in one chunk", () => {
    const chunk = Buffer.concat([
      encodeFrame(Buffer.from("a")),
      encodeFrame(Buffer.from("bb")),
      encodeFrame(Buffer.from("ccc")),
    ]);
    assert.deepEqual(texts(new FrameDecoder().push(chunk)), ["a", "bb", "ccc"]);
  });

  test("a frame split into two chunks at any byte is decoded once both arrive", () => {
    // The stream can be cut anywhere, including inside the 4-byte length prefix.
    const frame = encodeFrame(Buffer.from("split me"));
    for (let cut = 0; cut <= frame.length; cut++) {
      const decoder = new FrameDecoder();
      const first = decoder.push(frame.subarray(0, cut));
      const second = decoder.push(frame.subarray(cut));
      assert.deepEqual(texts([...first, ...second]), ["split me"], `cut at ${cut}`);
      // Nothing is handed out early: only the push that completes the frame returns it.
      assert.equal(first.length, cut === frame.length ? 1 : 0, `cut at ${cut}`);
    }
  });

  test("frames delivered one byte at a time are decoded in order", () => {
    // Includes a zero-length frame in the middle.
    const stream = Buffer.concat(["one", "", "three"].map((text) => encodeFrame(Buffer.from(text))));
    const decoder = new FrameDecoder();
    const frames: Buffer[] = [];
    for (let i = 0; i < stream.length; i++) {
      frames.push(...decoder.push(stream.subarray(i, i + 1)));
    }
    assert.deepEqual(texts(frames), ["one", "", "three"]);
  });

  test("an empty chunk changes nothing", () => {
    const decoder = new FrameDecoder();
    assert.deepEqual(decoder.push(Buffer.alloc(0)), []);
    assert.equal(decoder.bufferedBytes, 0);
  });

  test("bufferedBytes counts the bytes of a frame that isn't complete yet", () => {
    const frame = encodeFrame(Buffer.from("abcdef")); // 4 + 6 = 10 bytes
    const decoder = new FrameDecoder();
    decoder.push(frame.subarray(0, 2));
    assert.equal(decoder.bufferedBytes, 2); // half of the length prefix
    decoder.push(frame.subarray(2, 7));
    assert.equal(decoder.bufferedBytes, 7); // the prefix and 3 payload bytes
    decoder.push(frame.subarray(7));
    assert.equal(decoder.bufferedBytes, 0); // complete: nothing pending
  });

  test("returned frames don't share memory with the received chunks", () => {
    const chunk = encodeFrame(Buffer.from("abc"));
    const [frame] = new FrameDecoder().push(chunk);
    chunk.fill(0); // overwrite the received bytes
    assert.equal(frame?.toString(), "abc");
  });

  test("a frame of exactly the limit is accepted", () => {
    const frames = new FrameDecoder(10).push(encodeFrame(Buffer.alloc(10), 10));
    assert.deepEqual(
      frames.map((frame) => frame.length),
      [10],
    );
  });

  test("a frame over the limit is rejected as soon as its length arrives", () => {
    // Only the 4 length bytes are sent: the decoder must not wait for (or
    // reserve memory for) a payload it is going to refuse anyway.
    assert.throws(() => new FrameDecoder(10).push(lengthPrefix(11)), isProtocolError("FRAME_TOO_LARGE"));
    assert.throws(
      () => new FrameDecoder().push(lengthPrefix(DEFAULT_MAX_FRAME_BYTES + 1)),
      isProtocolError("FRAME_TOO_LARGE"),
    );
  });

  test("after a failure the decoder keeps rejecting, since frame boundaries are lost", () => {
    const decoder = new FrameDecoder(10);
    assert.throws(() => decoder.push(lengthPrefix(1_000)), isProtocolError("FRAME_TOO_LARGE"));
    // Even a perfectly valid frame is refused now: the stream is out of sync.
    assert.throws(() => decoder.push(encodeFrame(Buffer.from("ok"))), isProtocolError("FRAME_TOO_LARGE"));
  });

  test("an invalid limit is rejected", () => {
    for (const bad of [-1, 1.5, 2 ** 32]) {
      assert.throws(() => new FrameDecoder(bad), RangeError, `limit ${bad}`);
    }
  });
});
