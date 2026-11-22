import { createSlice, type PayloadAction } from "@reduxjs/toolkit";

import type { StatsStreamEvent, SystemStreamEvent } from "@shared/api/stream";

export type StreamConnection = "connecting" | "open" | "reconnecting" | "offline";

export type StreamState = {
  connection: StreamConnection;
  lastEventAt: string | null;
  statsRing: StatsStreamEvent[];
  system: SystemStreamEvent | null;
};

const STATS_RING_MAX = 300;

const initialState: StreamState = {
  connection: "offline",
  lastEventAt: null,
  statsRing: [],
  system: null,
};

const streamSlice = createSlice({
  name: "stream",
  initialState,
  reducers: {
    streamConnectionChanged(state, action: PayloadAction<StreamConnection>) {
      state.connection = action.payload;
    },
    streamEventReceived(state, action: PayloadAction<string>) {
      state.lastEventAt = action.payload;
    },
    streamStatsPushed(state, action: PayloadAction<StatsStreamEvent>) {
      state.statsRing.push(action.payload);
      if (state.statsRing.length > STATS_RING_MAX) {
        state.statsRing = state.statsRing.slice(-STATS_RING_MAX);
      }
      state.lastEventAt = new Date().toISOString();
    },
    streamSystemUpdated(state, action: PayloadAction<SystemStreamEvent>) {
      state.system = action.payload;
      state.lastEventAt = new Date().toISOString();
    },
    streamReset() {
      return initialState;
    },
  },
});

export const {
  streamConnectionChanged,
  streamEventReceived,
  streamStatsPushed,
  streamSystemUpdated,
  streamReset,
} = streamSlice.actions;
export const streamReducer = streamSlice.reducer;

export const selectStreamConnection = (state: { stream: StreamState }) => state.stream.connection;
export const selectStreamSystem = (state: { stream: StreamState }) => state.stream.system;
export const selectStatsRing = (state: { stream: StreamState }) => state.stream.statsRing;
