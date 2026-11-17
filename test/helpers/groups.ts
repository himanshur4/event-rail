// The consumer-group side of a test broker: an offset store under the test's data
// directory and a coordinator over it. The caller closes `offsets` after the server.

import { join } from "node:path";

import { GroupCoordinator, type GroupCoordinatorOptions } from "../../src/group/group-coordinator.js";
import { OffsetStore } from "../../src/group/offset-store.js";
import type { TopicManager } from "../../src/topic/topic-manager.js";

export async function openGroups(
  topics: TopicManager,
  dataDir: string,
  options: GroupCoordinatorOptions = {},
): Promise<{ groups: GroupCoordinator; offsets: OffsetStore }> {
  const offsets = await OffsetStore.open(join(dataDir, "internal", "offsets"), { syncOnAppend: false });
  return { groups: new GroupCoordinator(topics, offsets, options), offsets };
}
