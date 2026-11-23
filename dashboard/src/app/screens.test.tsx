import { MantineProvider } from "@mantine/core";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { Provider } from "react-redux";
import { RouterProvider } from "react-router-dom";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { router } from "@app/router";
import { appTheme } from "@app/theme";
import { createAppStore } from "@app/store";
import { sessionResolved } from "@features/session";
import "@features/session/api/authApi";

import { server } from "../test/msw";

class ResizeObserverStub {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

vi.stubGlobal("ResizeObserver", ResizeObserverStub);

const RULE = "22222222-2222-4222-8222-222222222222";
const ALERT = "11111111-1111-4111-8111-111111111111";
const TOKEN = "ab".repeat(32);

function alertRow(status: "open" | "acknowledged" = "open") {
  return {
    alert_id: ALERT,
    rule_id: RULE,
    status,
    merchant_id: null,
    card_token: TOKEN,
    event_id: "33333333-3333-4333-8333-333333333333",
    feature_snapshot: { card_cnt_1m: 8 },
    created_at: "2026-11-22T12:00:00.000Z",
    resolved_at: null,
    acknowledged_at: status === "acknowledged" ? "2026-11-22T12:01:00.000Z" : null,
    acknowledged_by: status === "acknowledged" ? "op-1" : null,
    resolved_by: null,
  };
}

function detail(status: "open" | "acknowledged" = "open") {
  return {
    alert: alertRow(status),
    rule: {
      rule_id: RULE,
      name: "card_velocity_burst",
      entity: "card",
      enabled: true,
      conditions: [{ feature_name: "card_cnt_1m", comparison: "gt", threshold: 5 }],
    },
    history:
      status === "open"
        ? []
        : [
            {
              audit_id: 1,
              occurred_at: "2026-11-22T12:01:00.000Z",
              actor: "op-1",
              action: "status_changed",
              detail: { from: "open", to: "acknowledged" },
            },
          ],
  };
}

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
  window.matchMedia = (query: string) =>
    ({
      matches: query.includes("min-width"),
      media: query,
      onchange: null,
      addListener: () => undefined,
      removeListener: () => undefined,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
      dispatchEvent: () => false,
    }) as MediaQueryList;
});
afterEach(() => {
  server.resetHandlers();
});
afterAll(() => {
  server.close();
});

describe("main screens", () => {
  it("walks overview, alerts, the alert card, and feature lookup", async () => {
    let status: "open" | "acknowledged" = "open";
    server.use(
      http.get("/api/v1/alerts", () =>
        HttpResponse.json({ items: [alertRow(status)], next_cursor: null }),
      ),
      http.get(`/api/v1/alerts/${ALERT}`, () => HttpResponse.json(detail(status))),
      http.patch(`/api/v1/alerts/${ALERT}`, async ({ request }) => {
        const body = (await request.json()) as { status?: "open" | "acknowledged" };
        status = body.status ?? "acknowledged";
        return HttpResponse.json(alertRow(status));
      }),
      http.get("/api/v1/rules", () =>
        HttpResponse.json([
          {
            rule_id: RULE,
            name: "card_velocity_burst",
            entity: "card",
            enabled: true,
            conditions: [{ feature_name: "card_cnt_1m", comparison: "gt", threshold: 5 }],
          },
        ]),
      ),
      http.get("/api/v1/catalog", () =>
        HttpResponse.json([
          {
            name: "card_cnt_1m",
            entity: "card",
            kind: "stream",
            agg: "count",
            default_value: 0,
            version: 1,
            window_ms: 60_000,
            stale_after_ms: 5_000,
          },
        ]),
      ),
      http.post("/api/v1/features", () =>
        HttpResponse.json({
          request_id: "44444444-4444-4444-8444-444444444444",
          features: {
            card_cnt_1m: {
              value: 8,
              status: "fresh",
              as_of: "2026-11-22T12:00:00.000Z",
              age_ms: 1000,
            },
          },
        }),
      ),
    );

    const store = createAppStore();
    store.dispatch(
      sessionResolved({
        operatorId: "op-1",
        email: "analyst@featurestore.local",
        role: "analyst",
      }),
    );
    render(
      <Provider store={store}>
        <MantineProvider theme={appTheme}>
          <RouterProvider router={router} />
        </MantineProvider>
      </Provider>,
    );

    await router.navigate("/");
    expect(await screen.findByRole("heading", { name: "Overview" })).toBeInTheDocument();
    expect(await screen.findByText("card_velocity_burst")).toBeInTheDocument();

    await router.navigate("/alerts");
    expect(await screen.findByRole("heading", { name: "Alerts" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Reset" }));
    fireEvent.change(screen.getByLabelText("Search"), { target: { value: "abcd" } });

    await router.navigate(`/alerts/${ALERT}`);
    expect(await screen.findByRole("heading", { name: "card_velocity_burst" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Take" }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Resolve" })).toBeEnabled();
    });
    fireEvent.click(screen.getByRole("button", { name: "Resolve" }));
    fireEvent.change(await screen.findByLabelText("Comment"), {
      target: { value: "Seen in the lab" },
    });
    fireEvent.click(
      screen.getAllByRole("button", { name: "Resolve" })[1] ??
        screen.getByRole("button", { name: "Resolve" }),
    );

    await router.navigate("/features");
    expect(await screen.findByRole("heading", { name: "Features" })).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Merchant id"), { target: { value: "m1" } });
    fireEvent.click(screen.getByRole("button", { name: "Look up" }));
    expect((await screen.findAllByText("card_cnt_1m")).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "Request again" }));
    expect(await screen.findByText(/request_id/i)).toBeInTheDocument();
  });
});
