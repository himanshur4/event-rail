// Commands for a live demo, run through npm:
//
//   npm run demo:reset    delete the demo data
//   npm run demo:broker   start a broker that stores its data in the demo folder
//   npm run demo:files    list the files the broker has written, with their sizes
//   npm run demo:crash    kill the broker abruptly, then leave half a batch at the
//                         end of a log file, as a crash during a write would
//
// The demo data lives in the system temp folder, so the project stays clean. The
// broker is used only through its command-line tool (dist/src/cli.js).

import { spawn } from "node:child_process";
import { appendFileSync, existsSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const DATA_DIR = join(tmpdir(), "eventrail-demo");
// Written by demo:broker so that demo:crash knows which process to kill.
const PID_FILE = join(tmpdir(), "eventrail-demo.pid");
// The file demo:crash damages: the first segment of orders partition 0.
const TORN_LOG = join(DATA_DIR, "topics", "orders", "0", "00000000000000000000.log");
// A batch length of 99 bytes followed by only 3 of them: a write cut short.
const TORN_BYTES = Buffer.from([0, 0, 0, 99, 1, 2, 3]);
// Relative to this file, so the commands work from any folder.
const CLI = fileURLToPath(new URL("../dist/src/cli.js", import.meta.url));

const command = process.argv[2];
switch (command) {
  case "reset":
    reset();
    break;
  case "broker":
    broker();
    break;
  case "files":
    files();
    break;
  case "crash":
    await crash();
    break;
  default:
    console.error("usage: node scripts/demo.mjs <reset | broker | files | crash>");
    process.exitCode = 1;
}


function reset() {
  // Windows can't delete files a running program has open, so the broker must
  // be stopped first.
  if (runningBrokerPid() !== null) {
    console.error("The demo broker is still running. Stop it first (Ctrl+C in its terminal).");
    process.exitCode = 1;
    return;
  }
  rmSync(DATA_DIR, { recursive: true, force: true });
  rmSync(PID_FILE, { force: true });
  console.log(`Demo data deleted: ${DATA_DIR}`);
}

function broker() {
  if (!existsSync(CLI)) {
    console.error("The CLI isn't built yet. Run: npm run build");
    process.exitCode = 1;
    return;
  }
  const child = spawn(process.execPath, [CLI, "broker", "--data", DATA_DIR], { stdio: "inherit" });
  writeFileSync(PID_FILE, String(child.pid));
  // Ctrl+C reaches the broker directly, which shuts down cleanly; this script
  // ignores it and waits for the broker to exit.
  process.on("SIGINT", () => {});
  child.on("exit", (code, signal) => {
    rmSync(PID_FILE, { force: true });
    if (signal !== null) {
      console.log(`(the broker process was killed: ${signal})`);
    }
    process.exitCode = code ?? 0;
  });
}

function files() {
  if (!existsSync(DATA_DIR)) {
    console.log("No demo data yet. Start the broker and create a topic first.");
    return;
  }
  console.log(`Files in ${DATA_DIR}:`);
  for (const path of listFiles(DATA_DIR)) {
    const size = statSync(path).size;
    console.log(`${String(size).padStart(8)} bytes  ${relative(DATA_DIR, path)}`);
  }
}

async function crash() {
  const pid = runningBrokerPid();
  if (pid === null) {
    console.error("No demo broker is running. Start one with: npm run demo:broker");
    process.exitCode = 1;
    return;
  }
  // SIGKILL ends the process at once, with no clean shutdown.
  process.kill(pid, "SIGKILL");
  await waitUntilStopped(pid);
  rmSync(PID_FILE, { force: true });
  console.log(`1. Killed the broker (process ${pid}) without any warning, like a power cut.`);

  if (!existsSync(TORN_LOG)) {
    console.log("2. Topic 'orders' has no data in partition 0 yet, so there's nothing to damage.");
    console.log("   Create the topic and produce a record with --key user-4, then try again.");
    return;
  }
  appendFileSync(TORN_LOG, TORN_BYTES);
  console.log(`2. Added ${TORN_BYTES.length} bytes to the end of orders partition 0: the start of a`);
  console.log("   batch that was never finished, exactly what a crash mid-write leaves behind.");
  console.log("3. Now start the broker again (npm run demo:broker) and watch it recover.");
}


// The PID of the demo broker if it's still running, otherwise null.
function runningBrokerPid() {
  if (!existsSync(PID_FILE)) {
    return null;
  }
  const pid = Number(readFileSync(PID_FILE, "utf8"));
  return Number.isInteger(pid) && isRunning(pid) ? pid : null;
}

// Signal 0 only checks that the process exists (EPERM: it exists but isn't ours).
function isRunning(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === "EPERM";
  }
}

// Waits up to 5 seconds, so the broker's files are closed before they are changed.
async function waitUntilStopped(pid) {
  for (let i = 0; i < 100 && isRunning(pid); i++) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

// Every file under `dir`, recursively, in sorted order.
function listFiles(dir) {
  const result = [];
  for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      result.push(...listFiles(path));
    } else {
      result.push(path);
    }
  }
  return result;
}
