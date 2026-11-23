import { configureStore, isAction } from "@reduxjs/toolkit";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { sessionReducer } from "@features/session";
import { baseApi } from "@shared/api/baseApi";
import type { components } from "@shared/api/schema";
import type { AlertStreamEvent } from "@shared/api/stream";

import {
  flushPendingAlerts,
  markAlertStreamGap,
  noteAlert,
  resetAlertCacheState,
  type AnyDispatch,
  type LiveState,
  type NoteAlertOptions,
} from "./alertCache";
import { OVERVIEW_ALERTS_QUERY } from "./overviewQuery";
import { streamReducer } from "./streamSlice";

type AlertOut = components["schemas"]["AlertOut"];

const RULE = "22222222-2222-4222-8222-222222222222";

function alertId(n: number): string {
  return `00000000-0000-4000-8000-${n.toString(16).padStart(12, "0")}`;
}

function listItem(id: string, status: AlertOut["status"] = "open"): AlertOut {
  return {
    alert_id: id,
    rule_id: RULE,
    status,
    merchant_id: null,
    card_token: "abcd…ef",
    event_id: "00000000-0000-4000-8000-000000000099",
    feature_snapshot: {},
    created_at: "2026-11-22T12:00:00.000Z",
    resolved_at: null,
    acknowledged_at: null,
  };
}

function streamEvent(
  n: number,
  op: AlertStreamEvent["op"] = "created",
  status: AlertStreamEvent["status"] = "open",
): AlertStreamEvent {
  return {
    id: n,
    op,
    alert_id: alertId(n),
    rule_id: RULE,
    rule_name: "card_velocity_burst",
    status,
    entity: "card",
    entity_id: "abcd…ef",
    created_at: "2026-11-22T12:00:00.000Z",
  };
}

function makeStore(onType?: (type: string) => void) {
  return configureStore({
    reducer: {
      session: sessionReducer,
      stream: streamReducer,
      [baseApi.reducerPath]: baseApi.reducer,
    },
    middleware: (getDefaultMiddleware) =>
      getDefaultMiddleware()
        .concat(baseApi.middleware)
        .concat(() => (next) => (action) => {
          if (onType && isAction(action)) {
            onType(action.type);
          }
          return next(action);
        }),
  });
}

async function seedAlerts(store: ReturnType<typeof makeStore>, items: AlertOut[]): Promise<void> {
  await store
    .dispatch(
      baseApi.util.upsertQueryData("getAlerts", OVERVIEW_ALERTS_QUERY, {
        items,
        next_cursor: null,
      }),
    )
    .unwrap();
}

function options(
  store: ReturnType<typeof makeStore>,
  now: number,
  notify: (message: string) => void = vi.fn(),
  schedule: NoteAlertOptions["schedule"] = vi.fn(),
): NoteAlertOptions {
  return {
    dispatch: store.dispatch as unknown as AnyDispatch,
    getState: () => store.getState() as LiveState,
    now,
    notify,
    schedule,
  };
}

describe("alert cache", () => {
  beforeEach(() => {
    resetAlertCacheState();
  });

  it("patches status and prepends created alerts on the first page", async () => {
    const store = makeStore();
    await seedAlerts(store, [listItem(alertId(1))]);
    noteAlert(streamEvent(1, "updated", "acknowledged"), options(store, 0));
    const page = () =>
      baseApi.endpoints.getAlerts.select(OVERVIEW_ALERTS_QUERY)(store.getState()).data;
    expect(page()?.items).toHaveLength(1);
    expect(page()?.items[0]?.status).toBe("acknowledged");

    for (let n = 2; n <= 12; n += 1) {
      noteAlert(streamEvent(n), options(store, n));
    }
    const items = page()?.items ?? [];
    expect(items).toHaveLength(10);
    expect(items[0]?.alert_id).toBe(alertId(12));
    expect(items.some((item) => item.alert_id === alertId(2))).toBe(false);
    expect(store.getState().stream.ruleNames[RULE]).toBe("card_velocity_burst");
  });

  it("buffers events that arrive before the REST page", async () => {
    const store = makeStore();
    noteAlert(streamEvent(3), options(store, 0));
    expect(
      baseApi.endpoints.getAlerts.select(OVERVIEW_ALERTS_QUERY)(store.getState()).data,
    ).toBeUndefined();
    await seedAlerts(store, []);
    flushPendingAlerts(store.dispatch as unknown as AnyDispatch);
    const items = baseApi.endpoints.getAlerts.select(OVERVIEW_ALERTS_QUERY)(store.getState()).data
      ?.items;
    expect(items?.map((item) => item.alert_id)).toEqual([alertId(3)]);
  });

  it("drops the buffer on reset and invalidates alerts", async () => {
    const types: string[] = [];
    const store = makeStore((type) => {
      types.push(type);
    });
    noteAlert(streamEvent(4), options(store, 0));
    markAlertStreamGap(store.dispatch as unknown as AnyDispatch);
    await seedAlerts(store, []);
    flushPendingAlerts(store.dispatch as unknown as AnyDispatch);
    expect(
      baseApi.endpoints.getAlerts.select(OVERVIEW_ALERTS_QUERY)(store.getState()).data?.items,
    ).toEqual([]);
    expect(types.some((type) => type.endsWith("invalidateTags"))).toBe(true);
  });

  it("holds live updates until the reload fulfills", async () => {
    const store = makeStore();
    await seedAlerts(store, [listItem(alertId(5))]);
    markAlertStreamGap(store.dispatch as unknown as AnyDispatch);
    await seedAlerts(store, [listItem(alertId(5))]);
    noteAlert(streamEvent(5, "updated", "resolved"), options(store, 0));
    const page = () =>
      baseApi.endpoints.getAlerts.select(OVERVIEW_ALERTS_QUERY)(store.getState()).data;
    expect(page()?.items[0]?.status).toBe("open");
    flushPendingAlerts(store.dispatch as unknown as AnyDispatch);
    expect(page()?.items[0]?.status).toBe("resolved");
  });

  it("updates an open alert card", async () => {
    const store = makeStore();
    const id = alertId(6);
    await store
      .dispatch(
        baseApi.util.upsertQueryData("getAlert", id, {
          alert: listItem(id),
          rule: {
            rule_id: RULE,
            name: "card_velocity_burst",
            entity: "card",
            enabled: true,
            conditions: [],
          },
          history: [],
        }),
      )
      .unwrap();
    noteAlert(streamEvent(6, "updated", "acknowledged"), options(store, 0));
    expect(baseApi.endpoints.getAlert.select(id)(store.getState()).data?.alert.status).toBe(
      "acknowledged",
    );
  });

  it("toasts a new alert at most once every 5s and clears the highlight", async () => {
    const store = makeStore();
    await seedAlerts(store, []);
    const messages: string[] = [];
    const runs: Array<() => void> = [];
    const notify = (message: string) => {
      messages.push(message);
    };
    const schedule: NoteAlertOptions["schedule"] = (run) => {
      runs.push(run);
    };
    noteAlert(streamEvent(7), options(store, 0, notify, schedule));
    noteAlert(streamEvent(8), options(store, 1_000, notify, schedule));
    expect(messages).toEqual(["New alert: card_velocity_burst"]);
    noteAlert(streamEvent(9), options(store, 5_000, notify, schedule));
    expect(messages).toHaveLength(2);
    noteAlert(streamEvent(7, "updated", "acknowledged"), options(store, 6_000, notify, schedule));
    expect(messages).toHaveLength(2);
    expect(store.getState().stream.highlightedAlertIds).toContain(alertId(7));
    runs[0]?.();
    expect(store.getState().stream.highlightedAlertIds).not.toContain(alertId(7));
  });

  it("queues new alerts for the list and patches status without prepending", async () => {
    const store = makeStore();
    const listArgs = { limit: 50, sort: "-created_at" as const };
    await store
      .dispatch(
        baseApi.util.upsertQueryData("getAlerts", listArgs, {
          items: [listItem(alertId(1))],
          next_cursor: null,
        }),
      )
      .unwrap();
    noteAlert(streamEvent(2), options(store, 0));
    const page = () => baseApi.endpoints.getAlerts.select(listArgs)(store.getState()).data;
    expect(page()?.items.map((item) => item.alert_id)).toEqual([alertId(1)]);
    expect(store.getState().stream.pendingCreated.map((item) => item.alert_id)).toEqual([
      alertId(2),
    ]);
    noteAlert(streamEvent(1, "updated", "acknowledged"), options(store, 1));
    expect(page()?.items[0]?.status).toBe("acknowledged");
    expect(page()?.items).toHaveLength(1);
  });
});
