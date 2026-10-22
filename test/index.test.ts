import { test } from "node:test";
import assert from "node:assert/strict";

import { VERSION } from "../src/index.js";

// Smoke test for the build and test pipeline.
test("VERSION is a semver-like string", () => {
  assert.match(VERSION, /^\d+\.\d+\.\d+$/);
});
