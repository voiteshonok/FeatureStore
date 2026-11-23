import { configureStore } from "@reduxjs/toolkit";

import { featureLookupReducer } from "@features/feature-lookup/model/featureLookupSlice";
import { sessionReducer } from "@features/session";
import { streamReducer } from "@features/stream";
import { createStreamListenerMiddleware } from "@features/stream/model/streamListener";
import { baseApi } from "@shared/api/baseApi";

export function createAppStore() {
  const streamListener = createStreamListenerMiddleware();
  return configureStore({
    reducer: {
      session: sessionReducer,
      stream: streamReducer,
      featureLookup: featureLookupReducer,
      [baseApi.reducerPath]: baseApi.reducer,
    },
    middleware: (getDefaultMiddleware) =>
      getDefaultMiddleware().prepend(streamListener).concat(baseApi.middleware),
  });
}

export type AppStore = ReturnType<typeof createAppStore>;
export type RootState = ReturnType<AppStore["getState"]>;
export type AppDispatch = AppStore["dispatch"];
