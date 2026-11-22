import { describe, expect, it } from "vitest";

import {
  streamAlertHighlightCleared,
  streamAlertHighlighted,
  streamChannelActivity,
  streamChannelStatus,
  streamReducer,
  streamReset,
  streamStatsPushed,
  streamSystemUpdated,
  type StatsPoint,
} from "./streamSlice";

function point(index: number): StatsPoint {
  return {
    events_per_s: index,
    events_per_s_avg_60s: index,
    alerts_60s_by_rule: {},
    active_cards_60s: index,
    receivedAt: new Date(Date.UTC(2026, 10, 22, 12, 0, index)).toISOString(),
  };
}

describe("streamReducer", () => {
  it("keeps the newest 300 stats points", () => {
    let state = streamReducer(undefined, { type: "init" });
    for (let index = 0; index < 301; index += 1) {
      state = streamReducer(state, streamStatsPushed(point(index)));
    }
    expect(state.statsRing).toHaveLength(300);
    expect(state.statsRing[0]?.events_per_s).toBe(1);
    expect(state.statsRing[299]?.events_per_s).toBe(300);
  });

  it("remembers when a channel left open", () => {
    const t0 = "2026-11-22T12:00:00.000Z";
    const t1 = "2026-11-22T12:00:01.000Z";
    const t2 = "2026-11-22T12:00:02.000Z";
    const t3 = "2026-11-22T12:00:03.000Z";
    let state = streamReducer(
      undefined,
      streamChannelStatus({ channel: "alerts", status: "connecting", at: t0 }),
    );
    expect(state.alertsChannel.notOpenSince).toBe(t0);
    state = streamReducer(
      state,
      streamChannelStatus({ channel: "alerts", status: "open", at: t1 }),
    );
    expect(state.alertsChannel.notOpenSince).toBeNull();
    expect(state.alertsChannel.lastActivityAt).toBe(t1);
    state = streamReducer(state, streamChannelActivity({ channel: "alerts", at: t2 }));
    expect(state.alertsChannel.lastActivityAt).toBe(t2);
    state = streamReducer(
      state,
      streamChannelStatus({ channel: "alerts", status: "reconnecting", at: t3 }),
    );
    expect(state.alertsChannel.notOpenSince).toBe(t3);
    state = streamReducer(
      state,
      streamChannelStatus({ channel: "alerts", status: "reconnecting", at: t1 }),
    );
    expect(state.alertsChannel.notOpenSince).toBe(t3);
  });

  it("resets live state", () => {
    let state = streamReducer(undefined, streamStatsPushed(point(1)));
    state = streamReducer(
      state,
      streamSystemUpdated({
        at: "2026-11-22T12:00:00.000Z",
        event: { mode: "normal", components: [] },
      }),
    );
    state = streamReducer(state, streamReset());
    expect(state.statsRing).toEqual([]);
    expect(state.system).toBeNull();
    expect(state.alertsChannel.status).toBe("offline");
  });

  it("clears a highlight", () => {
    let state = streamReducer(undefined, { type: "init" });
    state = streamReducer(state, streamAlertHighlighted("11111111-1111-4111-8111-111111111111"));
    state = streamReducer(
      state,
      streamAlertHighlightCleared("11111111-1111-4111-8111-111111111111"),
    );
    expect(state.highlightedAlertIds).toEqual([]);
  });
});
