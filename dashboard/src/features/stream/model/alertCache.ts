import type { SessionState } from "@features/session/model/sessionSlice";
import { baseApi, type AlertsListArgs } from "@shared/api/baseApi";
import type { components } from "@shared/api/schema";
import type { AlertStreamEvent } from "@shared/api/stream";

import { isOverviewAlertsQuery, OVERVIEW_ALERTS_QUERY } from "./overviewQuery";
import {
  streamAlertHighlightCleared,
  streamAlertHighlighted,
  streamRuleSeen,
  type StreamState,
} from "./streamSlice";

type AlertOut = components["schemas"]["AlertOut"];
type AlertList = components["schemas"]["AlertListOut"];

/** Stream rows have no snapshot; the card loads the real event id via REST. */
const STREAM_PLACEHOLDER_EVENT_ID = "00000000-0000-0000-0000-000000000000";
const HIGHLIGHT_MS = 8_000;
const TOAST_INTERVAL_MS = 5_000;
const MAX_PENDING = 50;

export type AnyDispatch = (action: unknown) => unknown;

export type LiveState = {
  session: SessionState;
  stream: StreamState;
  [baseApi.reducerPath]: ReturnType<typeof baseApi.reducer>;
};

export type NoteAlertOptions = {
  dispatch: AnyDispatch;
  getState: () => LiveState;
  now: number;
  notify: (message: string) => void;
  schedule: (run: () => void, delayMs: number) => void;
};

const pending: AlertStreamEvent[] = [];
let awaitReload = false;
let lastToastAt = Number.NEGATIVE_INFINITY;

export function resetAlertCacheState(): void {
  pending.length = 0;
  awaitReload = false;
  lastToastAt = Number.NEGATIVE_INFINITY;
}

export function clearPendingAlerts(): void {
  resetAlertCacheState();
}

/** `reset`: drop the gap and reload alerts from REST. Events until that reload stay buffered. */
export function markAlertStreamGap(dispatch: AnyDispatch): void {
  pending.length = 0;
  awaitReload = true;
  dispatch(baseApi.util.invalidateTags(["Alert"]));
}

export function noteAlert(event: AlertStreamEvent, options: NoteAlertOptions): void {
  options.dispatch(streamRuleSeen({ ruleId: event.rule_id, name: event.rule_name }));
  if (event.op === "created") {
    options.dispatch(streamAlertHighlighted(event.alert_id));
    options.schedule(() => {
      options.dispatch(streamAlertHighlightCleared(event.alert_id));
    }, HIGHLIGHT_MS);
    if (options.now - lastToastAt >= TOAST_INTERVAL_MS) {
      lastToastAt = options.now;
      options.notify(`New alert: ${event.rule_name}`);
    }
  }
  patchDetail(options.dispatch, event);
  if (awaitReload || !overviewCacheReady(options.getState())) {
    buffer(event);
    return;
  }
  patchList(options.dispatch, event);
}

export function flushPendingAlerts(dispatch: AnyDispatch): void {
  awaitReload = false;
  const batch = pending.splice(0);
  for (const event of batch) {
    patchList(dispatch, event);
  }
}

export function overviewArgsMatch(args: AlertsListArgs): boolean {
  return isOverviewAlertsQuery(args);
}

function buffer(event: AlertStreamEvent): void {
  pending.push(event);
  if (pending.length > MAX_PENDING) {
    pending.splice(0, pending.length - MAX_PENDING);
  }
}

function overviewCacheReady(state: LiveState): boolean {
  const selected = baseApi.endpoints.getAlerts.select(OVERVIEW_ALERTS_QUERY)(state);
  return selected.data !== undefined;
}

function patchList(dispatch: AnyDispatch, event: AlertStreamEvent): void {
  dispatch(
    baseApi.util.updateQueryData("getAlerts", OVERVIEW_ALERTS_QUERY, (draft) => {
      applyAlertEvent(draft, event, OVERVIEW_ALERTS_QUERY.limit);
    }),
  );
}

function patchDetail(dispatch: AnyDispatch, event: AlertStreamEvent): void {
  dispatch(
    baseApi.util.updateQueryData("getAlert", event.alert_id, (draft) => {
      draft.alert.status = event.status;
    }),
  );
}

function applyAlertEvent(draft: AlertList, event: AlertStreamEvent, limit: number): void {
  const index = draft.items.findIndex((item) => item.alert_id === event.alert_id);
  if (index >= 0) {
    const current = draft.items[index];
    if (current) {
      current.status = event.status;
    }
    return;
  }
  if (event.op !== "created") {
    return;
  }
  draft.items.unshift(streamAlertToListItem(event));
  if (draft.items.length > limit) {
    draft.items.length = limit;
  }
}

function streamAlertToListItem(event: AlertStreamEvent): AlertOut {
  return {
    alert_id: event.alert_id,
    rule_id: event.rule_id,
    status: event.status,
    merchant_id: event.entity === "merchant" ? event.entity_id : null,
    card_token: event.entity === "card" ? event.entity_id : null,
    event_id: STREAM_PLACEHOLDER_EVENT_ID,
    feature_snapshot: {},
    created_at: event.created_at,
    resolved_at: null,
    acknowledged_at: null,
  };
}
