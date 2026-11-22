import { configureStore } from "@reduxjs/toolkit";
import { http, HttpResponse } from "msw";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { sessionReducer } from "@features/session";
import { streamReducer } from "@features/stream";
import { notify } from "@shared/lib/notify";
import { server } from "../../test/msw";

import { baseApi } from "./baseApi";

vi.mock("@shared/lib/notify", () => ({ notify: vi.fn() }));

const id = "11111111-1111-4111-8111-111111111111";
const ruleId = "22222222-2222-4222-8222-222222222222";

function alert(status: "open" | "acknowledged") {
  return {
    alert_id: id,
    rule_id: ruleId,
    status,
    merchant_id: null,
    card_token: "abcdefghijklmnop",
    event_id: "33333333-3333-4333-8333-333333333333",
    feature_snapshot: { card_cnt_1m: 8 },
    created_at: "2026-11-22T12:00:00.000Z",
    resolved_at: null,
    acknowledged_at: status === "acknowledged" ? "2026-11-22T12:01:00.000Z" : null,
    acknowledged_by: status === "acknowledged" ? "op-9" : null,
    resolved_by: null,
  };
}

function detail(status: "open" | "acknowledged") {
  return {
    alert: alert(status),
    rule: {
      rule_id: ruleId,
      name: "card_velocity_burst",
      entity: "card",
      enabled: true,
      conditions: [{ feature_name: "card_cnt_1m", comparison: "gt", threshold: 5 }],
    },
    history:
      status === "acknowledged"
        ? [
            {
              audit_id: 4,
              occurred_at: "2026-11-22T12:01:00.000Z",
              actor: "op-9",
              action: "acknowledged",
              detail: { from: "open", to: "acknowledged" },
            },
          ]
        : [],
  };
}

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
});
afterEach(() => {
  server.resetHandlers();
  vi.mocked(notify).mockReset();
});
afterAll(() => {
  server.close();
});

describe("patchAlert", () => {
  it("rolls back a conflicting status change and names who changed it", async () => {
    const store = configureStore({
      reducer: {
        session: sessionReducer,
        stream: streamReducer,
        [baseApi.reducerPath]: baseApi.reducer,
      },
      middleware: (getDefaultMiddleware) => getDefaultMiddleware().concat(baseApi.middleware),
    });
    await store.dispatch(baseApi.util.upsertQueryData("getAlert", id, detail("open"))).unwrap();
    const optimistic = store.dispatch(
      baseApi.util.updateQueryData("getAlert", id, (draft) => {
        draft.alert.status = "acknowledged";
      }),
    );
    expect(baseApi.endpoints.getAlert.select(id)(store.getState()).data?.alert.status).toBe(
      "acknowledged",
    );
    optimistic.undo();
    expect(baseApi.endpoints.getAlert.select(id)(store.getState()).data?.alert.status).toBe("open");
    server.use(
      http.patch(`*/api/v1/alerts/${id}`, () =>
        HttpResponse.json(
          { type: "invalid_transition", title: "changed", status: 409, request_id: "r" },
          { status: 409 },
        ),
      ),
      http.get(`*/api/v1/alerts/${id}`, () => HttpResponse.json(detail("acknowledged"))),
    );
    await store.dispatch(
      baseApi.endpoints.patchAlert.initiate({ alertId: id, body: { status: "acknowledged" } }),
    );
    await vi.waitFor(() => {
      expect(notify).toHaveBeenCalledWith("Status was already changed by op-9", "warning");
    });
    expect(baseApi.endpoints.getAlert.select(id)(store.getState()).data?.alert.status).toBe(
      "acknowledged",
    );
  });
});
