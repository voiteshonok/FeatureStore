import { MantineProvider } from "@mantine/core";
import { Provider } from "react-redux";
import { RouterProvider } from "react-router-dom";

import { ErrorBoundary } from "@app/ErrorBoundary";
import { router } from "@app/router";
import { createAppStore } from "@app/store";
import { appTheme } from "@app/theme";

import "@mantine/core/styles.css";

const store = createAppStore();

export function AppProviders() {
  return (
    <Provider store={store}>
      <MantineProvider theme={appTheme} defaultColorScheme="light">
        <ErrorBoundary>
          <RouterProvider router={router} />
        </ErrorBoundary>
      </MantineProvider>
    </Provider>
  );
}
