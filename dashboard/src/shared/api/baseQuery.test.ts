import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { baseQueryWithErrors } from "./baseQuery";

const notify = vi.hoisted(() => vi.fn());
const emitSessionExpired = vi.hoisted(() => vi.fn());

vi.mock("@shared/lib/notify", () => ({ notify }));
vi.mock("@shared/lib/authEvents", () => ({
  emitSessionExpired,
  onSessionExpired: vi.fn(),
}));

function apiCtx() {
  return {
    signal: new AbortController().signal,
    abort: vi.fn(),
    dispatch: vi.fn(),
    getState: vi.fn(),
    extra: undefined,
    endpoint: "getCatalog",
    type: "query" as const,
    forced: false,
  };
}

describe("baseQueryWithErrors", () => {
  const assign = vi.fn();
  const originalFetch = globalThis.fetch;

  beforeEach(() => {
    notify.mockReset();
    emitSessionExpired.mockReset();
    assign.mockReset();
    vi.stubGlobal("location", {
      href: "http://localhost/alerts",
      pathname: "/alerts",
      search: "",
      origin: "http://localhost",
      assign,
    });
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.unstubAllGlobals();
  });

  it("redirects on 401 and emits session expired", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          type: "unauthorized",
          title: "invalid session",
          status: 401,
          request_id: "r1",
        }),
        { status: 401, headers: { "Content-Type": "application/json" } },
      ),
    );

    const result = await baseQueryWithErrors("/catalog", apiCtx(), {});

    expect(result.error?.status).toBe(401);
    expect(emitSessionExpired).toHaveBeenCalled();
    expect(assign).toHaveBeenCalledWith("/login?next=%2Falerts");
  });

  it("toasts on 403", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          type: "forbidden",
          title: "forbidden",
          status: 403,
          request_id: "r2",
        }),
        { status: 403, headers: { "Content-Type": "application/json" } },
      ),
    );

    await baseQueryWithErrors(
      { url: "/alerts/x", method: "PATCH", body: { status: "acknowledged" } },
      { ...apiCtx(), endpoint: "patchAlert", type: "mutation" },
      {},
    );

    expect(notify).toHaveBeenCalledWith("Insufficient permissions");
  });

  it("retries GET on 503 then toasts", async () => {
    const make503 = () =>
      new Response(
        JSON.stringify({
          type: "upstream_unavailable",
          title: "down",
          status: 503,
          request_id: "r3",
        }),
        { status: 503, headers: { "Content-Type": "application/json" } },
      );
    const fetchMock = vi.fn().mockImplementation(() => Promise.resolve(make503()));
    globalThis.fetch = fetchMock;

    await baseQueryWithErrors("/catalog", apiCtx(), {});

    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(notify).toHaveBeenCalledWith("Service unavailable (request_id: r3)");
  });
});
