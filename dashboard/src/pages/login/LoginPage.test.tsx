import { MantineProvider } from "@mantine/core";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { Provider } from "react-redux";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";

import { createAppStore } from "@app/store";

import "@features/session/api/authApi";
import { server } from "../../test/msw";
import LoginPage from "./LoginPage";

class ResizeObserverStub {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

vi.stubGlobal("ResizeObserver", ResizeObserverStub);

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
});
afterEach(() => {
  server.resetHandlers();
});
afterAll(() => {
  server.close();
});

function renderLogin(entry = "/login") {
  render(
    <Provider store={createAppStore()}>
      <MantineProvider>
        <MemoryRouter initialEntries={[entry]}>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/" element={<div>home</div>} />
            <Route path="/alerts" element={<div>alerts-home</div>} />
          </Routes>
        </MemoryRouter>
      </MantineProvider>
    </Provider>,
  );
}

describe("LoginPage", () => {
  it("does not call the API when the form is empty", async () => {
    let posts = 0;
    server.use(
      http.post("/api/v1/auth/login", () => {
        posts += 1;
        return new HttpResponse(null, { status: 204 });
      }),
    );
    renderLogin();
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));
    await waitFor(() => {
      expect(screen.getByText("Email is required")).toBeInTheDocument();
    });
    expect(screen.getByText("Password is required")).toBeInTheDocument();
    expect(posts).toBe(0);
  });

  it("shows an error for a wrong password", async () => {
    server.use(
      http.post("/api/v1/auth/login", () =>
        HttpResponse.json(
          { type: "unauthorized", title: "invalid", status: 401, request_id: "login" },
          { status: 401 },
        ),
      ),
    );
    renderLogin();
    fireEvent.change(screen.getByLabelText("Email"), {
      target: { value: "analyst@featurestore.local" },
    });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "wrong" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText("Invalid email or password")).toBeInTheDocument();
  });

  it("ignores an external next URL after a successful sign-in", async () => {
    server.use(
      http.post("/api/v1/auth/login", () => new HttpResponse(null, { status: 204 })),
      http.get("/api/v1/auth/me", () =>
        HttpResponse.json({
          operator_id: "op-1",
          email: "analyst@featurestore.local",
          role: "analyst",
        }),
      ),
    );
    renderLogin("/login?next=https://evil.example");
    fireEvent.change(screen.getByLabelText("Email"), {
      target: { value: "analyst@featurestore.local" },
    });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "demo-analyst-pass" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));
    expect(await screen.findByText("home")).toBeInTheDocument();
    expect(screen.queryByText("alerts-home")).not.toBeInTheDocument();
  });
});
