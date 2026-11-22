import { createSlice, type PayloadAction } from "@reduxjs/toolkit";

import type { LookupFormValues } from "./schema";

const MAX_RECENT = 5;

export type FeatureLookupState = {
  recent: LookupFormValues[];
};

const initialState: FeatureLookupState = {
  recent: [],
};

function sameLookup(left: LookupFormValues, right: LookupFormValues): boolean {
  return (
    left.cardToken === right.cardToken &&
    left.merchantId === right.merchantId &&
    left.lat === right.lat &&
    left.lon === right.lon &&
    left.ts === right.ts
  );
}

const featureLookupSlice = createSlice({
  name: "featureLookup",
  initialState,
  reducers: {
    featureLookupPushed(state, action: PayloadAction<LookupFormValues>) {
      state.recent = [
        action.payload,
        ...state.recent.filter((item) => !sameLookup(item, action.payload)),
      ].slice(0, MAX_RECENT);
    },
  },
});

export const { featureLookupPushed } = featureLookupSlice.actions;
export const featureLookupReducer = featureLookupSlice.reducer;

export const selectRecentLookups = (state: { featureLookup: FeatureLookupState }) =>
  state.featureLookup.recent;
