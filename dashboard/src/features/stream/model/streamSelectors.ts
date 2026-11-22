import { createSelector } from "@reduxjs/toolkit";

import { alertsInWindow, formatClock } from "./format";
import type { ChannelState, StreamConnection, StreamState } from "./streamSlice";

type StreamSliceState = { stream: StreamState };

const NOT_OPEN_MS = 3_000;
const SILENCE_MS = 20_000;

const HEALTH: Record<StreamConnection, number> = {
  open: 3,
  connecting: 2,
  reconnecting: 1,
  offline: 0,
};

export const selectAlertsChannel = (state: StreamSliceState) => state.stream.alertsChannel;
export const selectSystemChannel = (state: StreamSliceState) => state.stream.systemChannel;
export const selectStatsRing = (state: StreamSliceState) => state.stream.statsRing;
export const selectStreamSystem = (state: StreamSliceState) => state.stream.system;
export const selectRuleNames = (state: StreamSliceState) => state.stream.ruleNames;
export const selectHighlightedAlertIds = (state: StreamSliceState) =>
  state.stream.highlightedAlertIds;

export const selectStreamConnection = createSelector(
  [selectAlertsChannel, selectSystemChannel],
  (alerts, system): StreamConnection =>
    HEALTH[alerts.status] <= HEALTH[system.status] ? alerts.status : system.status,
);

const NO_EVENTS: { time: string; eventsPerS: number }[] = [];
const NO_BARS: { rule: string; count: number }[] = [];
const NO_NUMBERS: number[] = [];

export type StatSparklines = {
  eventsPerS: number[];
  alerts60s: number[];
  latencyP95: (number | null)[];
  activeCards: number[];
};

const EMPTY_SPARKS: StatSparklines = {
  eventsPerS: NO_NUMBERS,
  alerts60s: NO_NUMBERS,
  latencyP95: NO_NUMBERS,
  activeCards: NO_NUMBERS,
};

export const selectEventsSeries = createSelector([selectStatsRing], (ring) => {
  if (ring.length === 0) {
    return NO_EVENTS;
  }
  return ring.map((point) => ({
    time: formatClock(point.receivedAt),
    eventsPerS: point.events_per_s,
  }));
});

export const selectLatestStats = createSelector([selectStatsRing], (ring) => {
  if (ring.length === 0) {
    return null;
  }
  return ring[ring.length - 1] ?? null;
});

export const selectAlertsByRule = createSelector([selectLatestStats], (latest) => {
  if (!latest) {
    return NO_BARS;
  }
  const rows = Object.entries(latest.alerts_60s_by_rule).map(([rule, count]) => ({
    rule,
    count,
  }));
  rows.sort((left, right) => right.count - left.count || left.rule.localeCompare(right.rule));
  return rows;
});

export const selectStatSparklines = createSelector([selectStatsRing], (ring): StatSparklines => {
  if (ring.length === 0) {
    return EMPTY_SPARKS;
  }
  return {
    eventsPerS: ring.map((point) => point.events_per_s),
    alerts60s: ring.map((point) => alertsInWindow(point.alerts_60s_by_rule)),
    latencyP95: ring.map((point) => point.latency_p95_ms ?? null),
    activeCards: ring.map((point) => point.active_cards_60s),
  };
});

/** ISO time the live data went stale, or null while both channels are fresh. */
export function selectStaleSince(state: StreamSliceState, now: number): string | null {
  const alerts = channelStaleSince(state.stream.alertsChannel, now);
  const system = channelStaleSince(state.stream.systemChannel, now);
  if (alerts == null) {
    return system;
  }
  if (system == null) {
    return alerts;
  }
  return alerts < system ? alerts : system;
}

export function channelStaleSince(channel: ChannelState, now: number): string | null {
  if (channel.status !== "open") {
    if (channel.notOpenSince == null) {
      return null;
    }
    if (now - Date.parse(channel.notOpenSince) > NOT_OPEN_MS) {
      return channel.notOpenSince;
    }
    return null;
  }
  if (channel.lastActivityAt == null) {
    return null;
  }
  if (now - Date.parse(channel.lastActivityAt) > SILENCE_MS) {
    return channel.lastActivityAt;
  }
  return null;
}
