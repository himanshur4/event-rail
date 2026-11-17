/** Machine-readable reasons a consumer-group request was refused. */
export type GroupErrorCode =
  | "INVALID_GROUP_ID" // the group id breaks the naming rules
  | "UNKNOWN_MEMBER" // not in the group: removed after a timeout, or the broker restarted
  | "STALE_GENERATION" // the request carries an older generation than the group's
  | "REBALANCE_IN_PROGRESS" // the group is rebalancing, so the member must join again
  | "NOT_PARTITION_OWNER" // the member doesn't own that partition in this generation
  | "INCONSISTENT_GROUP_TOPIC"; // the group already reads a different topic

export class GroupError extends Error {
  readonly code: GroupErrorCode;

  constructor(code: GroupErrorCode, message: string) {
    super(message);
    this.name = "GroupError";
    this.code = code;
  }
}
