import { describe, expect, it, vi } from "vitest";

import { runSseStream } from "./sseClient";
import type { StreamConnection } from "./streamSlice";

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

describe("runSseStream", () => {
  it("uses server retry after a transport error and does not check the session", async () => {
    const confirmSession = vi.fn(async () => true);
    const abort = new AbortController();
    let calls = 0;
    const sleeps: number[] = [];
    await runSseStream({
      url: "/api/v1/stream/alerts",
      signal: abort.signal,
      confirmSession,
      random: () => 0,
      now: () => "2026-11-22T12:00:00.000Z",
      getLastEventId: () => null,
      setLastEventId: () => undefined,
      onStatus: () => undefined,
      onActivity: () => undefined,
      onEvent: () => undefined,
      onInvalid: () => undefined,
      sleep: (ms) => {
        sleeps.push(ms);
        abort.abort();
        return Promise.resolve();
      },
      fetchImpl: () => {
        calls += 1;
        if (calls === 1) {
          return Promise.reject(new TypeError("network"));
        }
        return Promise.resolve(sseBody(""));
      },
    });
    expect(confirmSession).not.toHaveBeenCalled();
    expect(sleeps).toEqual([3_000]);
  });

  it("stops on HTTP 401 when the session check fails", async () => {
    const confirmSession = vi.fn(async () => false);
    let calls = 0;
    await runSseStream({
      url: "/api/v1/stream/alerts",
      signal: new AbortController().signal,
      confirmSession,
      random: () => 0,
      now: () => "2026-11-22T12:00:00.000Z",
      getLastEventId: () => null,
      setLastEventId: () => undefined,
      onStatus: () => undefined,
      onActivity: () => undefined,
      onEvent: () => undefined,
      onInvalid: () => undefined,
      sleep: () => Promise.resolve(),
      fetchImpl: () => {
        calls += 1;
        return Promise.resolve(new Response("no", { status: 401 }));
      },
    });
    expect(calls).toBe(1);
    expect(confirmSession).toHaveBeenCalledOnce();
  });

  it("backs off after an HTTP error when the session is still valid", async () => {
    const abort = new AbortController();
    const sleeps: number[] = [];
    const statuses: StreamConnection[] = [];
    await runSseStream({
      url: "/api/v1/stream/alerts",
      signal: abort.signal,
      confirmSession: async () => true,
      random: () => 0,
      now: () => "2026-11-22T12:00:00.000Z",
      getLastEventId: () => null,
      setLastEventId: () => undefined,
      onStatus: (status) => {
        statuses.push(status);
      },
      onActivity: () => undefined,
      onEvent: () => undefined,
      onInvalid: () => undefined,
      sleep: (ms) => {
        sleeps.push(ms);
        abort.abort();
        return Promise.resolve();
      },
      fetchImpl: () => Promise.resolve(new Response("busy", { status: 503 })),
    });
    expect(sleeps).toEqual([500]);
    expect(statuses).toContain("reconnecting");
  });

  it("sends Last-Event-ID on the next connection", async () => {
    const abort = new AbortController();
    let lastId: string | null = null;
    let calls = 0;
    await runSseStream({
      url: "/api/v1/stream/alerts",
      signal: abort.signal,
      confirmSession: async () => true,
      random: () => 0,
      now: () => "2026-11-22T12:00:00.000Z",
      getLastEventId: () => lastId,
      setLastEventId: (id) => {
        lastId = id;
      },
      onStatus: () => undefined,
      onActivity: () => undefined,
      onEvent: () => undefined,
      onInvalid: () => undefined,
      sleep: () => Promise.resolve(),
      fetchImpl: (_url, init) => {
        calls += 1;
        const header = new Headers(init?.headers).get("Last-Event-ID");
        if (calls === 1) {
          expect(header).toBeNull();
          return Promise.resolve(sseBody('event: alert\nid: 15\ndata: {"ok":true}\n\n'));
        }
        expect(header).toBe("15");
        abort.abort();
        return Promise.reject(new DOMException("The operation was aborted.", "AbortError"));
      },
    });
    expect(calls).toBe(2);
    expect(lastId).toBe("15");
  });

  it("skips an invalid frame and keeps the connection", async () => {
    const abort = new AbortController();
    const events: string[] = [];
    const invalid: string[] = [];
    await runSseStream({
      url: "/api/v1/stream/alerts",
      signal: abort.signal,
      confirmSession: async () => true,
      random: () => 0,
      now: () => "2026-11-22T12:00:00.000Z",
      getLastEventId: () => null,
      setLastEventId: () => undefined,
      onStatus: () => undefined,
      onActivity: () => undefined,
      onEvent: (name) => {
        events.push(name);
      },
      onInvalid: (name) => {
        invalid.push(name);
      },
      sleep: () => {
        abort.abort();
        return Promise.resolve();
      },
      fetchImpl: () =>
        Promise.resolve(
          sseBody('event: alert\ndata: {nope}\n\nevent: stats\ndata: {"ok":true}\n\n'),
        ),
    });
    expect(invalid).toEqual(["alert"]);
    expect(events).toEqual(["stats"]);
  });

  it("honours the server retry field", async () => {
    const abort = new AbortController();
    const sleeps: number[] = [];
    await runSseStream({
      url: "/api/v1/stream/system",
      signal: abort.signal,
      confirmSession: async () => true,
      random: () => 0,
      now: () => "2026-11-22T12:00:00.000Z",
      getLastEventId: () => null,
      setLastEventId: () => undefined,
      onStatus: () => undefined,
      onActivity: () => undefined,
      onEvent: () => undefined,
      onInvalid: () => undefined,
      sleep: (ms) => {
        sleeps.push(ms);
        abort.abort();
        return Promise.resolve();
      },
      fetchImpl: () => Promise.resolve(sseBody("retry: 5000\n\n")),
    });
    expect(sleeps).toEqual([5_000]);
  });
});
