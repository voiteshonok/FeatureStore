import { createSlice, type PayloadAction } from "@reduxjs/toolkit";

import type { StatsStreamEvent, SystemStreamEvent } from "@shared/api/stream";

export type StreamConnection = "connecting" | "open" | "reconnecting" | "offline";

export type StreamChannelName = "alerts" | "system";

export type ChannelState = {
  status: StreamConnection;
  /** When this channel last left `open`, or when connecting started. */
  notOpenSince: string | null;
  /** Last SSE frame, including `: ping`. */
  lastActivityAt: string | null;
};

export type StatsPoint = StatsStreamEvent & { receivedAt: string };

export type StreamState = {
  alertsChannel: ChannelState;
  systemChannel: ChannelState;
  statsRing: StatsPoint[];
  system: SystemStreamEvent | null;
  ruleNames: Record<string, string>;
  highlightedAlertIds: string[];
};

const STATS_RING_MAX = 300;
const HIGHLIGHT_MAX = 20;

function idleChannel(): ChannelState {
  return { status: "offline", notOpenSince: null, lastActivityAt: null };
}

function freshState(): StreamState {
  return {
    alertsChannel: idleChannel(),
    systemChannel: idleChannel(),
    statsRing: [],
    system: null,
    ruleNames: {},
    highlightedAlertIds: [],
  };
}

const streamSlice = createSlice({
  name: "stream",
  initialState: freshState(),
  reducers: {
    streamChannelStatus(
      state,
      action: PayloadAction<{ channel: StreamChannelName; status: StreamConnection; at: string }>,
    ) {
      const channel = channelState(state, action.payload.channel);
      const next = action.payload.status;
      if (next === "open") {
        channel.status = "open";
        channel.notOpenSince = null;
        channel.lastActivityAt = action.payload.at;
        return;
      }
      const wasOpen = channel.status === "open";
      channel.status = next;
      if (wasOpen || channel.notOpenSince == null) {
        channel.notOpenSince = action.payload.at;
      }
    },
    streamChannelActivity(
      state,
      action: PayloadAction<{ channel: StreamChannelName; at: string }>,
    ) {
      channelState(state, action.payload.channel).lastActivityAt = action.payload.at;
    },
    streamStatsPushed(state, action: PayloadAction<StatsPoint>) {
      state.statsRing.push(action.payload);
      if (state.statsRing.length > STATS_RING_MAX) {
        state.statsRing.splice(0, state.statsRing.length - STATS_RING_MAX);
      }
      state.alertsChannel.lastActivityAt = action.payload.receivedAt;
    },
    streamSystemUpdated(state, action: PayloadAction<{ event: SystemStreamEvent; at: string }>) {
      state.system = action.payload.event;
      state.systemChannel.lastActivityAt = action.payload.at;
    },
    streamRuleSeen(state, action: PayloadAction<{ ruleId: string; name: string }>) {
      state.ruleNames[action.payload.ruleId] = action.payload.name;
    },
    streamAlertHighlighted(state, action: PayloadAction<string>) {
      const id = action.payload;
      state.highlightedAlertIds = [
        id,
        ...state.highlightedAlertIds.filter((item) => item !== id),
      ].slice(0, HIGHLIGHT_MAX);
    },
    streamAlertHighlightCleared(state, action: PayloadAction<string>) {
      state.highlightedAlertIds = state.highlightedAlertIds.filter(
        (item) => item !== action.payload,
      );
    },
    streamReset() {
      return freshState();
    },
  },
});

function channelState(state: StreamState, name: StreamChannelName): ChannelState {
  return name === "alerts" ? state.alertsChannel : state.systemChannel;
}

export const {
  streamChannelStatus,
  streamChannelActivity,
  streamStatsPushed,
  streamSystemUpdated,
  streamRuleSeen,
  streamAlertHighlighted,
  streamAlertHighlightCleared,
  streamReset,
} = streamSlice.actions;
export const streamReducer = streamSlice.reducer;
