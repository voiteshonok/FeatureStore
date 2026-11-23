import { createListenerMiddleware, type ListenerMiddlewareInstance } from "@reduxjs/toolkit";

import { authApi } from "@features/session/api/authApi";
import { selectIsAuthenticated, type SessionState } from "@features/session/model/sessionSlice";
import { baseApi } from "@shared/api/baseApi";
import { logger } from "@shared/lib/logger";
import { notify } from "@shared/lib/notify";

import {
  clearPendingAlerts,
  flushPendingAlerts,
  markAlertStreamGap,
  noteAlert,
  overviewArgsMatch,
  type AnyDispatch,
  type LiveState,
} from "./alertCache";
import { runSseStream } from "./sseClient";
import { acceptAlertsEvent, acceptSystemEvent } from "./streamEvents";
import {
  streamChannelActivity,
  streamChannelStatus,
  streamReset,
  streamStatsPushed,
  streamSystemUpdated,
  type StreamChannelName,
  type StreamConnection,
} from "./streamSlice";

type StartListening = ListenerMiddlewareInstance<unknown, AnyDispatch>["startListening"];

export function sessionBecameAuthenticated(
  previous: { session: SessionState },
  current: { session: SessionState },
): boolean {
  return selectIsAuthenticated(current) && !selectIsAuthenticated(previous);
}

export function registerStreamListeners(startListening: StartListening): void {
  startListening({
    predicate: (_action, currentState, previousState) =>
      sessionBecameAuthenticated(
        previousState as { session: SessionState },
        currentState as { session: SessionState },
      ),
    effect: async (_action, api) => {
      // Vitest stores authenticate through MSW; opening SSE there loops forever.
      if (import.meta.env.MODE === "test") {
        return;
      }
      const abort = new AbortController();
      const timers = new Set<ReturnType<typeof setTimeout>>();
      const dispatch = api.dispatch;
      const getState = () => api.getState() as LiveState;
      const running = connectLiveStreams({
        dispatch,
        getState,
        signal: abort.signal,
        registerTimer: (timer) => {
          timers.add(timer);
        },
        unregisterTimer: (timer) => {
          timers.delete(timer);
        },
      });
      try {
        await api.condition(
          (_next, state) => !selectIsAuthenticated(state as { session: SessionState }),
        );
      } catch {
        // Listener cancelled with the store.
      } finally {
        abort.abort();
        for (const timer of timers) {
          clearTimeout(timer);
        }
        clearPendingAlerts();
        if (!selectIsAuthenticated(getState())) {
          dispatch(streamReset());
        }
        await running;
      }
    },
  });

  startListening({
    matcher: baseApi.endpoints.getAlerts.matchFulfilled,
    effect: (action, api) => {
      if (!overviewArgsMatch(action.meta.arg.originalArgs)) {
        return;
      }
      flushPendingAlerts(api.dispatch);
    },
  });
}

type ConnectCtx = {
  dispatch: AnyDispatch;
  getState: () => LiveState;
  signal: AbortSignal;
  registerTimer: (timer: ReturnType<typeof setTimeout>) => void;
  unregisterTimer: (timer: ReturnType<typeof setTimeout>) => void;
};

export function connectLiveStreams(ctx: ConnectCtx): Promise<void> {
  let lastAlertId: string | null = null;
  const alerts = runSseStream({
    url: "/api/v1/stream/alerts",
    signal: ctx.signal,
    getLastEventId: () => lastAlertId,
    setLastEventId: (id) => {
      lastAlertId = id;
    },
    onStatus: (status) => {
      publishStatus(ctx, "alerts", status);
    },
    onActivity: (at) => {
      ctx.dispatch(streamChannelActivity({ channel: "alerts", at }));
    },
    onInvalid: () => {
      logger.warn("skip invalid alerts event");
    },
    confirmSession: () => confirmSession(ctx),
    onEvent: (name, data) => {
      const accepted = acceptAlertsEvent(name, data);
      if (accepted.kind === "invalid") {
        logger.warn("skip invalid alerts event");
        return;
      }
      if (accepted.kind === "ignore") {
        return;
      }
      if (accepted.kind === "stats") {
        ctx.dispatch(
          streamStatsPushed({
            ...accepted.event,
            receivedAt: new Date().toISOString(),
          }),
        );
        return;
      }
      if (accepted.kind === "reset") {
        lastAlertId = null;
        markAlertStreamGap(ctx.dispatch);
        return;
      }
      noteAlert(accepted.event, {
        dispatch: ctx.dispatch,
        getState: ctx.getState,
        now: Date.now(),
        notify: (message) => {
          notify(message, "info");
        },
        schedule: (run, delayMs) => {
          const timer = setTimeout(() => {
            ctx.unregisterTimer(timer);
            run();
          }, delayMs);
          ctx.registerTimer(timer);
        },
      });
    },
  });

  const system = runSseStream({
    url: "/api/v1/stream/system",
    signal: ctx.signal,
    getLastEventId: () => null,
    setLastEventId: () => undefined,
    onStatus: (status) => {
      publishStatus(ctx, "system", status);
    },
    onActivity: (at) => {
      ctx.dispatch(streamChannelActivity({ channel: "system", at }));
    },
    onInvalid: () => {
      logger.warn("skip invalid system event");
    },
    confirmSession: () => confirmSession(ctx),
    onEvent: (name, data) => {
      const accepted = acceptSystemEvent(name, data);
      if (accepted.kind === "invalid") {
        logger.warn("skip invalid system event");
        return;
      }
      if (accepted.kind === "snapshot") {
        const at = new Date().toISOString();
        ctx.dispatch(streamSystemUpdated({ event: accepted.event, at }));
      }
    },
  });

  return Promise.all([alerts, system]).then(() => undefined);
}

function publishStatus(
  ctx: ConnectCtx,
  channel: StreamChannelName,
  status: StreamConnection,
): void {
  ctx.dispatch(streamChannelStatus({ channel, status, at: new Date().toISOString() }));
}

async function confirmSession(ctx: ConnectCtx): Promise<boolean> {
  try {
    const initiated = ctx.dispatch(
      authApi.endpoints.getMe.initiate(undefined, { forceRefetch: true }),
    );
    if (isUnwrap(initiated)) {
      await initiated.unwrap();
    }
  } catch {
    return false;
  }
  return selectIsAuthenticated(ctx.getState());
}

function isUnwrap(value: unknown): value is { unwrap: () => Promise<unknown> } {
  return (
    typeof value === "object" &&
    value !== null &&
    "unwrap" in value &&
    typeof value.unwrap === "function"
  );
}

export function createStreamListenerMiddleware() {
  const listener = createListenerMiddleware();
  registerStreamListeners(listener.startListening);
  return listener.middleware;
}
