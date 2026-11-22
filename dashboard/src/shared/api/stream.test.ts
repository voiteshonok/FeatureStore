import { describe, expect, it } from "vitest";

import { parseStatsStreamEvent, parseSystemStreamEvent } from "./stream";

describe("stream schemas", () => {
  it("accepts a valid stats event", () => {
    const parsed = parseStatsStreamEvent({
      events_per_s: 12.5,
      events_per_s_avg_60s: 10,
      alerts_60s_by_rule: { velocity: 2 },
      latency_p50_ms: 4,
      latency_p95_ms: 9,
      active_cards_60s: 3,
    });
    expect(parsed?.events_per_s).toBe(12.5);
  });

  it("rejects invalid system events", () => {
    expect(parseSystemStreamEvent({ mode: "weird", components: [] })).toBeNull();
  });
});
