import { beforeEach, describe, expect, it, vi } from "vitest";

import { createAppStore } from "@app/store";
import { sessionResolved } from "@features/session";
import "@features/session/api/authApi";

import { resetAlertCacheState } from "./alertCache";
import { connectLiveStreams } from "./streamListener";

vi.mock("@shared/lib/notify", () => ({ notify: vi.fn() }));

const RULE = "22222222-2222-4222-8222-222222222222";
const ALERT = "11111111-1111-4111-8111-111111111111";

function sseBody(text: string, status = 200): Response {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
      controller.close();
    },
  });
  return new Response(stream, {
    status,
    headers: { "content-type": "text/event-stream" },
  });
}

const alertsFrame = [
  "id: 7",
  "event: stats",
  `data: ${JSON.stringify({
    events_per_s: 1,
    events_per_s_avg_60s: 1,
    alerts_60s_by_rule: { card_velocity_burst: 1 },
    latency_p50_ms: 4,
    latency_p95_ms: 9,
    active_cards_60s: 2,
  })}`,
  "",
  "event: reset",
  "data: {}",
  "",
  "event: alert",
  `data: ${JSON.stringify({
    id: 1,
    op: "created",
    alert_id: ALERT,
    rule_id: RULE,
    rule_name: "card_velocity_burst",
    status: "open",
    entity: "card",
    entity_id: "abcd…ef",
    created_at: "2026-11-22T12:00:00.000Z",
  })}`,
  "",
  "event: other",
  "data: {}",
  "",
  "event: stats",
  "data: {",
  "",
].join("\n");

const systemFrame = [
  "event: snapshot",
  `data: ${JSON.stringify({
    mode: "normal",
    components: [
      { name: "ingestion", status: "up", checked_at: "2026-11-22T12:00:00.000Z", lag: null },
    ],
  })}`,
  "",
  "event: change",
  "data: {",
  "",
].join("\n");

describe("connectLiveStreams", () => {
  beforeEach(() => {
    resetAlertCacheState();
  });

  it("applies stats, alerts, reset, and a system snapshot", async () => {
    const store = createAppStore();
    store.dispatch(
      sessionResolved({
        operatorId: "op-1",
        email: "analyst@featurestore.local",
        role: "analyst",
      }),
    );
    const original = globalThis.fetch;
    const abort = new AbortController();
    globalThis.fetch = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/stream/system")) {
        return Promise.resolve(sseBody(systemFrame));
      }
      return Promise.resolve(sseBody(alertsFrame));
    }) as typeof fetch;
    const done = connectLiveStreams({
      dispatch: store.dispatch,
      getState: () => store.getState(),
      signal: abort.signal,
      registerTimer: () => undefined,
      unregisterTimer: () => undefined,
    });
    try {
      await vi.waitFor(() => {
        expect(store.getState().stream.statsRing.length).toBeGreaterThan(0);
      });
      abort.abort();
      await done;
    } finally {
      globalThis.fetch = original;
    }
    expect(store.getState().stream.statsRing.length).toBeGreaterThan(0);
    expect(store.getState().stream.system?.mode).toBe("normal");
  });

  it("stops when the session check fails", async () => {
    const store = createAppStore();
    const original = globalThis.fetch;
    globalThis.fetch = vi.fn((input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/auth/me")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({ type: "unauthorized", title: "no", status: 401, request_id: "s" }),
            { status: 401, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.resolve(new Response("no", { status: 401 }));
    }) as typeof fetch;
    try {
      await connectLiveStreams({
        dispatch: store.dispatch,
        getState: () => store.getState(),
        signal: new AbortController().signal,
        registerTimer: () => undefined,
        unregisterTimer: () => undefined,
      });
    } finally {
      globalThis.fetch = original;
    }
    expect(store.getState().stream.statsRing).toHaveLength(0);
  });
});
