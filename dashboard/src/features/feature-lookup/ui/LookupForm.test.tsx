import { MantineProvider } from "@mantine/core";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { LookupForm } from "./LookupForm";
import { emptyLookupForm } from "../model/schema";

class ResizeObserverStub {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

vi.stubGlobal("ResizeObserver", ResizeObserverStub);

describe("LookupForm", () => {
  it("does not submit a card number", async () => {
    const onSubmit = vi.fn();
    const initial = emptyLookupForm(Date.parse("2026-11-22T12:00:00.000Z"));
    render(
      <MantineProvider>
        <LookupForm initial={initial} submitting={false} onSubmit={onSubmit} />
      </MantineProvider>,
    );
    fireEvent.change(screen.getByLabelText("Card token"), {
      target: { value: "4111111111111111" },
    });
    fireEvent.change(screen.getByLabelText("Merchant id"), { target: { value: "shop" } });
    fireEvent.click(screen.getByRole("button", { name: "Look up" }));
    await waitFor(() => {
      expect(screen.getByText("A token is required, not a card number")).toBeInTheDocument();
    });
    expect(onSubmit).not.toHaveBeenCalled();
  });

  it("submits a merchant id without a card token", async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    const initial = emptyLookupForm(Date.now());
    render(
      <MantineProvider>
        <LookupForm initial={initial} submitting={false} onSubmit={onSubmit} />
      </MantineProvider>,
    );
    fireEvent.change(screen.getByLabelText("Merchant id"), { target: { value: "m1868" } });
    fireEvent.click(screen.getByRole("button", { name: "Look up" }));
    await waitFor(() => {
      expect(onSubmit).toHaveBeenCalled();
    });
    const [values] = onSubmit.mock.calls[0] ?? [];
    expect(values).toMatchObject({ cardToken: "", merchantId: "m1868" });
  });

  it("does not submit when both identifiers are empty", async () => {
    const onSubmit = vi.fn();
    const initial = emptyLookupForm(Date.now());
    render(
      <MantineProvider>
        <LookupForm initial={initial} submitting={false} onSubmit={onSubmit} />
      </MantineProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Look up" }));
    await waitFor(() => {
      expect(screen.getByText("Enter a card token or a merchant id")).toBeInTheDocument();
    });
    expect(onSubmit).not.toHaveBeenCalled();
  });
});
