import type { TopicInfo } from "../protocol/messages.js";
import type { Connection } from "./connection.js";

/** Creates and lists topics through a broker connection. */
export class Admin {
  private readonly connection: Connection;

  constructor(connection: Connection) {
    this.connection = connection;
  }

  /**
   * Creates a topic. Rejects with BrokerError TOPIC_EXISTS, INVALID_TOPIC_NAME
   * or INVALID_PARTITION_COUNT if the broker refuses.
   */
  async createTopic(name: string, partitionCount: number): Promise<void> {
    await this.connection.send({ type: "createTopic", topic: name, partitionCount });
  }

  /** Every topic on the broker with its partition count, sorted by name. */
  async listTopics(): Promise<readonly TopicInfo[]> {
    const response = await this.connection.send({ type: "metadata" });
    return response.topics;
  }
}
