import type { SystemStreamEvent } from "@shared/api/stream";

export const SYSTEM_COMPONENT_ORDER = [
  "ingestion",
  "features-worker",
  "features-offline",
  "features-api",
  "detection",
  "gateway",
  "Kafka",
  "Redis",
  "PostgreSQL",
  "ClickHouse",
] as const satisfies readonly SystemStreamEvent["components"][number]["name"][];

export function systemModeCopy(mode: SystemStreamEvent["mode"]): string | null {
  if (mode === "degraded") {
    return "Degraded mode";
  }
  if (mode === "fallback") {
    return "Fallback mode";
  }
  return null;
}

export function componentStatusCopy(
  status: SystemStreamEvent["components"][number]["status"],
): string {
  if (status === "up") {
    return "Up";
  }
  if (status === "down") {
    return "Down";
  }
  return "Unknown";
}

export function componentStatusColor(
  status: SystemStreamEvent["components"][number]["status"],
): string {
  if (status === "up") {
    return "green";
  }
  if (status === "down") {
    return "red";
  }
  return "gray";
}
