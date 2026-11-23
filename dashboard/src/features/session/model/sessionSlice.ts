import { createSlice, type PayloadAction } from "@reduxjs/toolkit";

export type OperatorRole = "viewer" | "analyst" | "admin";

export type SessionStatus = "unknown" | "authenticated" | "anonymous";

export type SessionState = {
  status: SessionStatus;
  operatorId: string | null;
  email: string | null;
  role: OperatorRole | null;
};

const initialState: SessionState = {
  status: "unknown",
  operatorId: null,
  email: null,
  role: null,
};

type AuthenticatedPayload = {
  operatorId: string;
  email: string;
  role: OperatorRole;
};

const sessionSlice = createSlice({
  name: "session",
  initialState,
  reducers: {
    sessionResolved(state, action: PayloadAction<AuthenticatedPayload>) {
      state.status = "authenticated";
      state.operatorId = action.payload.operatorId;
      state.email = action.payload.email;
      state.role = action.payload.role;
    },
    sessionAnonymous(state) {
      state.status = "anonymous";
      state.operatorId = null;
      state.email = null;
      state.role = null;
    },
    sessionCleared() {
      return initialState;
    },
  },
});

export const { sessionResolved, sessionAnonymous, sessionCleared } = sessionSlice.actions;
export const sessionReducer = sessionSlice.reducer;

export const selectSessionStatus = (state: { session: SessionState }) => state.session.status;
export const selectIsAuthenticated = (state: { session: SessionState }) =>
  state.session.status === "authenticated";
export const selectCanChangeAlerts = (state: { session: SessionState }) =>
  state.session.role === "analyst" || state.session.role === "admin";
