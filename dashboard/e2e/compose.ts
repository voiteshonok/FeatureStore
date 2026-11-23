import { execFile } from "node:child_process";
import path from "node:path";

const root = path.resolve(process.cwd(), "..");

export function compose(args: string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile(
      "docker",
      ["compose", "-f", "infra/docker-compose.yml", ...args],
      { cwd: root },
      (error, stdout, stderr) => {
        if (error) {
          reject(new Error(stderr || stdout || error.message));
          return;
        }
        resolve(stdout);
      },
    );
  });
}

export async function burstCard(): Promise<void> {
  const stdout = await compose([
    "--profile",
    "sim",
    "run",
    "--build",
    "--rm",
    "simulator",
    "burst",
    "--card-index",
    "0",
    "--count",
    "12",
    "--seed",
    "42",
    "--url",
    "http://ingestion:8000",
  ]);
  const line = stdout
    .trim()
    .split("\n")
    .reverse()
    .find((row) => row.startsWith("{"));
  const stats = JSON.parse(line ?? "{}") as { accepted?: number };
  if (!stats.accepted) {
    throw new Error(`burst accepted nothing: ${stdout}`);
  }
}
