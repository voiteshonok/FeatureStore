import { describe, expect, it } from "vitest";

import {
  channelStaleSince,
  selectAlertsByRule,
  selectEventsSeries,
  selectStaleSince,
} from "./streamSelectors";
import {
  streamChannelActivity,
  streamChannelStatus,
  streamReducer,
  streamStatsPushed,
  type ChannelState,
  type StatsPoint,
  type StreamState,
} from "./streamSlice";

const T0 = Date.parse("2026-11-22T12:00:00.000Z");

function at(offsetMs: number): string {
  return new Date(T0 + offsetMs).toISOString();
}

function withStream(stream: StreamState) {
  return { stream };
}

describe("selectStaleSince", () => {
  it("stays fresh until a channel has been down for more than 3s", () => {
    const connecting = streamReducer(
      undefined,
      streamChannelStatus({ channel: "alerts", status: "connecting", at: at(0) }),
    );
    expect(selectStaleSince(withStream(connecting), T0 + 3_000)).toBeNull();
    expect(selectStaleSince(withStream(connecting), T0 + 3_001)).toBe(at(0));
  });

  it("treats an open channel with a recent ping as fresh", () => {
    let state = streamReducer(
      undefined,
      streamChannelStatus({ channel: "alerts", status: "open", at: at(0) }),
    );
    state = streamReducer(state, streamChannelActivity({ channel: "alerts", at: at(0) }));
    expect(selectStaleSince(withStream(state), T0 + 20_000)).toBeNull();
    expect(selectStaleSince(withStream(state), T0 + 20_001)).toBe(at(0));
    state = streamReducer(state, streamChannelActivity({ channel: "alerts", at: at(19_000) }));
    expect(selectStaleSince(withStream(state), T0 + 20_001)).toBeNull();
  });

  it("ignores a logged-out channel that never started", () => {
    const idle: ChannelState = { status: "offline", notOpenSince: null, lastActivityAt: null };
    expect(channelStaleSince(idle, T0 + 60_000)).toBeNull();
  });

  it("reports the older of two stale channels", () => {
    let state = streamReducer(
      undefined,
      streamChannelStatus({ channel: "alerts", status: "reconnecting", at: at(5_000) }),
    );
    state = streamReducer(
      state,
      streamChannelStatus({ channel: "system", status: "reconnecting", at: at(0) }),
    );
    expect(selectStaleSince(withStream(state), T0 + 10_000)).toBe(at(0));
  });
});

describe("chart selectors", () => {
  it("keeps a stable empty series and sorts rule bars", () => {
    const empty = streamReducer(undefined, { type: "init" });
    const first = selectEventsSeries(withStream(empty));
    expect(selectEventsSeries(withStream(empty))).toBe(first);

    const stats: StatsPoint = {
      events_per_s: 4,
      events_per_s_avg_60s: 3,
      alerts_60s_by_rule: { beta: 1, alpha: 3 },
      latency_p95_ms: 12,
      active_cards_60s: 2,
      receivedAt: at(0),
    };
    const filled = streamReducer(empty, streamStatsPushed(stats));
    const series = selectEventsSeries(withStream(filled));
    expect(series).toHaveLength(1);
    expect(series[0]?.eventsPerS).toBe(4);
    expect(selectAlertsByRule(withStream(filled))).toEqual([
      { rule: "alpha", count: 3 },
      { rule: "beta", count: 1 },
    ]);
  });
});
