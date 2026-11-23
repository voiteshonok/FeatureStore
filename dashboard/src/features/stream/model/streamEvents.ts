import {
  parseAlertStreamEvent,
  parseResetStreamEvent,
  parseStatsStreamEvent,
  parseSystemStreamEvent,
  type AlertStreamEvent,
  type ResetStreamEvent,
  type StatsStreamEvent,
  type SystemStreamEvent,
} from "@shared/api/stream";

export type AcceptedAlerts =
  | { kind: "alert"; event: AlertStreamEvent }
  | { kind: "stats"; event: StatsStreamEvent }
  | { kind: "reset"; event: ResetStreamEvent }
  | { kind: "ignore" }
  | { kind: "invalid" };

export function acceptAlertsEvent(name: string, data: unknown): AcceptedAlerts {
  if (name === "alert") {
    const event = parseAlertStreamEvent(data);
    return event ? { kind: "alert", event } : { kind: "invalid" };
  }
  if (name === "stats") {
    const event = parseStatsStreamEvent(data);
    return event ? { kind: "stats", event } : { kind: "invalid" };
  }
  if (name === "reset") {
    const event = parseResetStreamEvent(data);
    return event ? { kind: "reset", event } : { kind: "invalid" };
  }
  return { kind: "ignore" };
}

export type AcceptedSystem =
  { kind: "snapshot"; event: SystemStreamEvent } | { kind: "ignore" } | { kind: "invalid" };

export function acceptSystemEvent(name: string, data: unknown): AcceptedSystem {
  if (name === "snapshot" || name === "change") {
    const event = parseSystemStreamEvent(data);
    return event ? { kind: "snapshot", event } : { kind: "invalid" };
  }
  return { kind: "ignore" };
}
