import { describe, expect, it } from "vitest";

import { consumeSseFrames } from "./parseSse";

describe("consumeSseFrames", () => {
  it("reads retry, ping, and an event id across chunks", () => {
    const retry = consumeSseFrames("retry: 3000\n\n");
    expect(retry.frames).toEqual([{ kind: "retry", retryMs: 3000 }]);

    const partial = consumeSseFrames('event: alert\nid: 42\ndata: {"x":1}');
    expect(partial.frames).toEqual([]);
    const rest = consumeSseFrames(`${partial.rest}\n\n: ping\n\n`);
    expect(rest.frames).toEqual([
      { kind: "event", event: "alert", data: '{"x":1}', id: "42" },
      { kind: "ping" },
    ]);
  });

  it("accepts CRLF frames", () => {
    const parsed = consumeSseFrames("event: stats\r\ndata: {}\r\n\r\n");
    expect(parsed.frames).toEqual([{ kind: "event", event: "stats", data: "{}" }]);
  });
});
