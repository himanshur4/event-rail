import { MemoryLog } from "../../src/storage/memory-log.js";
import { runLogContractTests } from "./log-contract.js";

runLogContractTests("MemoryLog", async (clock) => new MemoryLog({ clock }));
