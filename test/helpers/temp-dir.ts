// Gives each test its own empty directory, so files left by one test can't
// affect another, and removes them all once the tests finish.

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const createdDirs: string[] = [];

/** Creates a new, empty, uniquely named directory for one test. */
export async function createTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "eventrail-test-"));
  createdDirs.push(dir);
  return dir;
}

/** Deletes every directory created by createTempDir. Call once, after all tests. */
export async function removeTempDirs(): Promise<void> {
  await Promise.all(createdDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
}
