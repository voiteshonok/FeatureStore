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

  it("toasts on 404 and 500", async () => {
    globalThis.fetch = vi
      .fn()
      .mockResolvedValue(
        new Response(
          JSON.stringify({ type: "not_found", title: "missing", status: 404, request_id: "r4" }),
          { status: 404, headers: { "Content-Type": "application/json" } },
        ),
      );
    await baseQueryWithErrors("/alerts/missing", apiCtx(), {});
    expect(notify).toHaveBeenCalledWith("Not found");

    notify.mockReset();
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          type: "internal_error",
          title: "boom",
          status: 500,
          request_id: "r5",
        }),
        { status: 500, headers: { "Content-Type": "application/json" } },
      ),
    );
    await baseQueryWithErrors(
      { url: "/alerts/x", method: "PATCH", body: { status: "acknowledged" } },
      { ...apiCtx(), type: "mutation" },
      {},
    );
    expect(notify).toHaveBeenCalledWith("Service unavailable (request_id: r5)");
  });

  it("returns 422 without a toast", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          type: "validation_error",
          title: "invalid",
          status: 422,
          request_id: "r6",
          errors: [{ field: "card_token", message: "required" }],
        }),
        { status: 422, headers: { "Content-Type": "application/json" } },
      ),
    );
    const result = await baseQueryWithErrors(
      { url: "/features", method: "POST", body: {} },
      { ...apiCtx(), type: "mutation" },
      {},
    );
    expect(result.error?.status).toBe(422);
    expect(notify).not.toHaveBeenCalled();
  });

  it("toasts a retry delay on 429", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          type: "rate_limited",
          title: "slow down",
          status: 429,
          request_id: "r7",
        }),
        { status: 429, headers: { "Content-Type": "application/json", "Retry-After": "3" } },
      ),
    );
    await baseQueryWithErrors(
      { url: "/auth/login", method: "POST", body: {} },
      { ...apiCtx(), type: "mutation" },
      {},
    );
    expect(notify).toHaveBeenCalledWith("Too many requests. Try again in 3s", "warning");
  });

  it("toasts on 409 unless the caller silences it", async () => {
    const conflict = () =>
      new Response(
        JSON.stringify({
          type: "conflict",
          title: "changed",
          status: 409,
          request_id: "r8",
        }),
        { status: 409, headers: { "Content-Type": "application/json" } },
      );
    globalThis.fetch = vi.fn().mockResolvedValue(conflict());
    await baseQueryWithErrors(
      { url: "/alerts/x", method: "PATCH", body: { status: "acknowledged" } },
      { ...apiCtx(), type: "mutation" },
      {},
    );
    expect(notify).toHaveBeenCalledWith("Conflict — refresh and try again", "warning");

    notify.mockReset();
    globalThis.fetch = vi.fn().mockResolvedValue(conflict());
    await baseQueryWithErrors(
      { url: "/alerts/x", method: "PATCH", body: { status: "acknowledged" } },
      { ...apiCtx(), type: "mutation" },
      { silentStatuses: [409] },
    );
    expect(notify).not.toHaveBeenCalled();
  });
});
