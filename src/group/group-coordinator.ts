// Tracks the members of every consumer group, decides which member reads which
// partition, and refuses requests from members whose view of the group is out of date.
//
// Membership: a member stays in its group while it keeps making requests
// (heartbeats, joins, commits). One that is silent for sessionTimeoutMs is removed.
//
// Rebalancing: when a member joins or leaves, every member must stop reading and
// join again before anyone gets a new assignment. Until then, joins answer
// "waiting" and heartbeats answer REBALANCE_IN_PROGRESS. Members that haven't
// rejoined after rebalanceTimeoutMs are removed. So no two live members are ever
// handed the same partition at the same time.
//
// Generations: each completed rebalance increments the group's generation.
// Heartbeats and commits carry the member's generation, and an older one is
// refused (STALE_GENERATION). A member that was removed while paused, or whose
// request was delayed, therefore can't overwrite the partition's new owner's progress.
//
// Membership lives in memory: after a broker restart, members join again.
// Committed offsets are durable (offset-store.ts). Time comes only from the
// injected clock, and timeouts are applied by tick(), so tests control time exactly.

import { LogError } from "../storage/log.js";
import type { TopicManager } from "../topic/topic-manager.js";
import { TopicError } from "../topic/topic-metadata.js";
import { GroupError } from "./group-error.js";
import type { OffsetStore } from "./offset-store.js";

export interface GroupCoordinatorOptions {
  /** A member silent for longer than this is removed (default 10 s). */
  readonly sessionTimeoutMs?: number;
  /** How long a rebalance waits for every member to join again (default 10 s). */
  readonly rebalanceTimeoutMs?: number;
  /** Returns the current time in milliseconds. Injectable so tests control time. */
  readonly clock?: () => number;
}

/** The answer to a join: "waiting" while the group rebalances, then the member's assignment. */
export interface JoinResult {
  readonly memberId: string;
  readonly status: "waiting" | "assigned";
  /** The assignment's generation; 0 while waiting. */
  readonly generation: number;
  /** The partitions this member reads; empty while waiting, or if there are more members than partitions. */
  readonly partitions: readonly number[];
}

interface Group {
  topic: string;
  partitionCount: number;
  // Member id → time of its last request. Insertion order is join order.
  readonly members: Map<string, number>;
  // The last completed generation, and the assignment it produced.
  generation: number;
  assignment: Map<string, readonly number[]>;
  // The members that have joined again during a rebalance; null when the group is stable.
  rebalance: { readonly ready: Set<string>; readonly deadline: number } | null;
}

const MAX_GROUP_ID_LENGTH = 100;
const GROUP_ID_PATTERN = /^[a-z0-9][a-z0-9._-]*$/;

/** Membership, assignment and offset commits for every consumer group on this broker. */
export class GroupCoordinator {
  private readonly topics: TopicManager;
  private readonly offsets: OffsetStore;
  private readonly sessionTimeoutMs: number;
  private readonly rebalanceTimeoutMs: number;
  private readonly clock: () => number;
  private readonly groups = new Map<string, Group>();
  private nextMemberNumber = 1;

  constructor(topics: TopicManager, offsets: OffsetStore, options: GroupCoordinatorOptions = {}) {
    this.topics = topics;
    this.offsets = offsets;
    this.sessionTimeoutMs = options.sessionTimeoutMs ?? 10_000;
    this.rebalanceTimeoutMs = options.rebalanceTimeoutMs ?? 10_000;
    this.clock = options.clock ?? (() => Date.now());
  }

  /**
   * Joins `groupId` to read `topic`, with `memberId` "" for a new member. Answers
   * "waiting" until every member has joined again; the caller then asks again with
   * the member id it was given. Throws INVALID_GROUP_ID, INCONSISTENT_GROUP_TOPIC,
   * UNKNOWN_MEMBER, or UNKNOWN_TOPIC.
   */
  join(groupId: string, topic: string, memberId: string): JoinResult {
    validateGroupId(groupId);
    const now = this.clock();
    const group = this.groupFor(groupId, topic);

    let id = memberId;
    if (id === "") {
      id = `member-${this.nextMemberNumber++}`;
      group.members.set(id, now);
      this.startRebalance(group, now);
    } else if (group.members.has(id)) {
      group.members.set(id, now);
    } else {
      throw unknownMember(groupId, id);
    }

    if (group.rebalance !== null) {
      group.rebalance.ready.add(id);
      this.completeIfReady(group);
    }
    if (group.rebalance !== null) {
      return { memberId: id, status: "waiting", generation: 0, partitions: [] };
    }
    return { memberId: id, status: "assigned", generation: group.generation, partitions: group.assignment.get(id) ?? [] };
  }

  /**
   * Records that the member is alive. Throws UNKNOWN_MEMBER, REBALANCE_IN_PROGRESS
   * (the member must stop reading and join again), or STALE_GENERATION.
   */
  heartbeat(groupId: string, memberId: string, generation: number): void {
    const group = this.memberGroup(groupId, memberId);
    if (group.rebalance !== null) {
      throw new GroupError("REBALANCE_IN_PROGRESS", `group ${groupId} is rebalancing; join again`);
    }
    checkGeneration(groupId, group, generation);
  }

  /** Removes the member at once, so its partitions move without waiting for a timeout. */
  leave(groupId: string, memberId: string): void {
    const group = this.memberGroup(groupId, memberId);
    this.removeMember(group, memberId);
  }

  /**
   * Commits `offset` as the next offset the group reads from the partition. Only the
   * partition's owner in the current generation may commit, and the offset must be
   * within the partition's log. Resolves once the commit is durable.
   */
  // Allowed during a rebalance too: the old assignment stands until the new one is
  // complete, so members can save their progress before joining again.
  async commit(
    groupId: string,
    memberId: string,
    generation: number,
    topic: string,
    partition: number,
    offset: number,
  ): Promise<void> {
    const group = this.memberGroup(groupId, memberId);
    checkGeneration(groupId, group, generation);
    const owned = group.assignment.get(memberId) ?? [];
    if (topic !== group.topic || !owned.includes(partition)) {
      throw new GroupError(
        "NOT_PARTITION_OWNER",
        `${memberId} doesn't own ${topic} partition ${partition} in generation ${generation}`,
      );
    }
    const log = this.topics.partition(topic, partition);
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > log.logEndOffset) {
      throw new LogError(
        "OFFSET_OUT_OF_RANGE",
        `offset ${offset} is outside ${topic} partition ${partition}, which ends at ${log.logEndOffset}`,
      );
    }
    await this.offsets.commit(groupId, topic, partition, offset);
  }

  /** The group's committed offset for the partition, or null if it has none. Needs no membership. */
  fetchOffset(groupId: string, topic: string, partition: number): number | null {
    validateGroupId(groupId);
    this.topics.partition(topic, partition); // UNKNOWN_TOPIC / UNKNOWN_PARTITION
    return this.offsets.get(groupId, topic, partition);
  }

  /** Removes silent members and members that didn't rejoin in time. Call it regularly. */
  tick(): void {
    const now = this.clock();
    for (const group of this.groups.values()) {
      for (const [id, lastSeen] of [...group.members]) {
        if (now - lastSeen > this.sessionTimeoutMs) {
          this.removeMember(group, id);
        }
      }
      const rebalance = group.rebalance;
      if (rebalance !== null && now >= rebalance.deadline) {
        for (const id of [...group.members.keys()]) {
          if (!rebalance.ready.has(id)) {
            this.removeMember(group, id);
          }
        }
      }
    }
  }

  // An empty group may switch to another topic; one with members may not.
  private groupFor(groupId: string, topic: string): Group {
    const existing = this.groups.get(groupId);
    if (existing !== undefined && existing.members.size > 0) {
      if (existing.topic !== topic) {
        throw new GroupError(
          "INCONSISTENT_GROUP_TOPIC",
          `group ${groupId} reads topic ${existing.topic}, not ${topic}`,
        );
      }
      return existing;
    }
    const partitionCount = this.partitionCount(topic);
    if (existing !== undefined) {
      existing.topic = topic;
      existing.partitionCount = partitionCount;
      return existing;
    }
    const group: Group = { topic, partitionCount, members: new Map(), generation: 0, assignment: new Map(), rebalance: null };
    this.groups.set(groupId, group);
    return group;
  }

  private partitionCount(topic: string): number {
    const info = this.topics.listTopics().find((t) => t.name === topic);
    if (info === undefined) {
      throw new TopicError("UNKNOWN_TOPIC", `topic ${topic} does not exist`);
    }
    return info.partitionCount;
  }

  // The member's group, after recording that the member is alive.
  private memberGroup(groupId: string, memberId: string): Group {
    const group = this.groups.get(groupId);
    if (group === undefined || !group.members.has(memberId)) {
      throw unknownMember(groupId, memberId);
    }
    group.members.set(memberId, this.clock());
    return group;
  }

  private removeMember(group: Group, id: string): void {
    group.members.delete(id);
    if (group.members.size === 0) {
      // The generation keeps counting, so requests from the old members stay stale.
      group.rebalance = null;
      group.assignment = new Map();
      return;
    }
    if (group.rebalance === null) {
      this.startRebalance(group, this.clock());
    } else {
      group.rebalance.ready.delete(id);
      this.completeIfReady(group);
    }
  }

  private startRebalance(group: Group, now: number): void {
    if (group.rebalance === null) {
      group.rebalance = { ready: new Set(), deadline: now + this.rebalanceTimeoutMs };
    }
  }

  // Once every member has joined again, the new generation and assignment take effect.
  private completeIfReady(group: Group): void {
    const rebalance = group.rebalance;
    if (rebalance === null) {
      return;
    }
    for (const id of group.members.keys()) {
      if (!rebalance.ready.has(id)) {
        return;
      }
    }
    group.generation += 1;
    group.assignment = assignRoundRobin([...group.members.keys()], group.partitionCount);
    group.rebalance = null;
  }
}

/**
 * Deals partitions out in turn: partition p goes to member p % memberCount, with
 * members in the given order. Members beyond the partition count get none.
 */
export function assignRoundRobin(memberIds: readonly string[], partitionCount: number): Map<string, number[]> {
  const assignment = new Map<string, number[]>(memberIds.map((id) => [id, []]));
  for (let partition = 0; partition < partitionCount; partition++) {
    const owner = memberIds[partition % memberIds.length];
    if (owner !== undefined) {
      assignment.get(owner)?.push(partition);
    }
  }
  return assignment;
}

function validateGroupId(groupId: string): void {
  if (groupId.length > MAX_GROUP_ID_LENGTH || !GROUP_ID_PATTERN.test(groupId)) {
    throw new GroupError(
      "INVALID_GROUP_ID",
      `group id ${JSON.stringify(groupId)} must be 1 to ${MAX_GROUP_ID_LENGTH} lowercase letters, digits, '.', '_' or '-', starting with a letter or digit`,
    );
  }
}

function checkGeneration(groupId: string, group: Group, generation: number): void {
  if (generation !== group.generation) {
    throw new GroupError(
      "STALE_GENERATION",
      `generation ${generation} is out of date: group ${groupId} is at generation ${group.generation}`,
    );
  }
}

function unknownMember(groupId: string, memberId: string): GroupError {
  return new GroupError("UNKNOWN_MEMBER", `${memberId || "(no id)"} is not a member of group ${groupId}; join again`);
}
