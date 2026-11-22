import { MantineProvider } from "@mantine/core";
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { Provider } from "react-redux";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { createAppStore } from "@app/store";
import { selectSessionStatus } from "@features/session";

import "@features/session/api/authApi";
import { server } from "../../../test/msw";
import { SessionBootstrap } from "./SessionBootstrap";

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
});
afterEach(() => {
  server.resetHandlers();
});
afterAll(() => {
  server.close();
});

function renderBootstrap() {
  const store = createAppStore();
  render(
    <Provider store={store}>
      <MantineProvider>
        <SessionBootstrap>
          <div>app-ready</div>
        </SessionBootstrap>
      </MantineProvider>
    </Provider>,
  );
  return store;
}

describe("SessionBootstrap", () => {
  it("becomes anonymous on 401 without flashing protected content", async () => {
    server.use(
      http.get("/api/v1/auth/me", () =>
        HttpResponse.json(
          { type: "unauthorized", title: "missing", status: 401, request_id: "t" },
          { status: 401 },
        ),
      ),
    );
    const store = renderBootstrap();
    expect(screen.getByLabelText(/loading page/i)).toBeInTheDocument();
    await waitFor(() => {
      expect(selectSessionStatus(store.getState())).toBe("anonymous");
    });
    expect(screen.getByText("app-ready")).toBeInTheDocument();
  });

  it("becomes authenticated on 200", async () => {
    server.use(
      http.get("/api/v1/auth/me", () =>
        HttpResponse.json({
          operator_id: "op-1",
          email: "analyst@featurestore.local",
          role: "analyst",
        }),
      ),
    );
    const store = renderBootstrap();
    await waitFor(() => {
      expect(selectSessionStatus(store.getState())).toBe("authenticated");
    });
    expect(screen.getByText("app-ready")).toBeInTheDocument();
  });
});
