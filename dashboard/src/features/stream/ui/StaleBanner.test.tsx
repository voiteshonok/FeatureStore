import { MantineProvider } from "@mantine/core";
import { act, render, screen } from "@testing-library/react";
import { Provider } from "react-redux";
import { describe, expect, it } from "vitest";

import { createAppStore } from "@app/store";
import { sessionAnonymous, sessionResolved } from "@features/session";
import { streamChannelStatus } from "@features/stream/model/streamSlice";

import { StaleBanner } from "./StaleBanner";

function renderBanner() {
  const store = createAppStore();
  render(
    <Provider store={store}>
      <MantineProvider>
        <StaleBanner />
      </MantineProvider>
    </Provider>,
  );
  return store;
}

describe("StaleBanner", () => {
  it("announces a stale stream for a signed-in operator", () => {
    const store = renderBanner();
    act(() => {
      store.dispatch(sessionResolved({ operatorId: "1", email: "a@x", role: "analyst" }));
      store.dispatch(
        streamChannelStatus({
          channel: "alerts",
          status: "reconnecting",
          at: new Date(Date.now() - 10_000).toISOString(),
        }),
      );
    });
    expect(screen.getByRole("status")).toHaveTextContent(/Data is not updating since/i);
    expect(screen.getByRole("status")).toHaveAttribute("aria-live", "polite");
  });

  it("stays hidden on the login screen", () => {
    const store = renderBanner();
    act(() => {
      store.dispatch(sessionAnonymous());
      store.dispatch(
        streamChannelStatus({
          channel: "alerts",
          status: "reconnecting",
          at: new Date(Date.now() - 10_000).toISOString(),
        }),
      );
    });
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });
});
