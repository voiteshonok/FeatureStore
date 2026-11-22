import { configureStore } from "@reduxjs/toolkit";

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
      [baseApi.reducerPath]: baseApi.reducer,
    },
    middleware: (getDefaultMiddleware) =>
      getDefaultMiddleware().prepend(streamListener).concat(baseApi.middleware),
  });
}

export type AppStore = ReturnType<typeof createAppStore>;
export type RootState = ReturnType<AppStore["getState"]>;
export type AppDispatch = AppStore["dispatch"];
