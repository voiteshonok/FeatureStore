import { useEffect, type ReactNode } from "react";

import { MantineProvider } from "@mantine/core";
import { Notifications } from "@mantine/notifications";
import { Provider, useDispatch } from "react-redux";
import { RouterProvider } from "react-router-dom";

import { ErrorBoundary } from "@app/ErrorBoundary";
import { router } from "@app/router";
import { createAppStore, type AppDispatch } from "@app/store";
import { appTheme } from "@app/theme";
import { SessionBootstrap, sessionAnonymous } from "@features/session";
import { baseApi } from "@shared/api/baseApi";
import { onSessionExpired } from "@shared/lib/authEvents";

import "@mantine/core/styles.css";
import "@mantine/notifications/styles.css";
import "@app/contrast.css";

// Ensure auth endpoints are registered before first query.
import "@features/session/api/authApi";

const store = createAppStore();

if (import.meta.env.DEV) {
  const win = window as unknown as {
    __fsPatchAlert?: (alertId: string, status: "acknowledged" | "resolved" | "open") => void;
  };
  win.__fsPatchAlert = (alertId, status) => {
    void store.dispatch(baseApi.endpoints.patchAlert.initiate({ alertId, body: { status } }));
  };
}

function SessionExpiredBridge({ children }: { children: ReactNode }) {
  const dispatch = useDispatch<AppDispatch>();
  useEffect(() => {
    return onSessionExpired(() => {
      dispatch(sessionAnonymous());
    });
  }, [dispatch]);
  return children;
}

export function AppProviders() {
  return (
    <Provider store={store}>
      <MantineProvider theme={appTheme} defaultColorScheme="auto">
        <Notifications position="top-right" />
        <ErrorBoundary>
          <SessionExpiredBridge>
            <SessionBootstrap>
              <RouterProvider router={router} />
            </SessionBootstrap>
          </SessionExpiredBridge>
        </ErrorBoundary>
      </MantineProvider>
    </Provider>
  );
}
