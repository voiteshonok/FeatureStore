import { MantineProvider } from "@mantine/core";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ResolveDialog } from "./ResolveDialog";

class ResizeObserverStub {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

vi.stubGlobal("ResizeObserver", ResizeObserverStub);

describe("ResolveDialog", () => {
  it("does not submit without a comment", () => {
    const onResolve = vi.fn();
    render(
      <MantineProvider>
        <ResolveDialog
          opened
          liveStatus="open"
          baselineStatus="open"
          submitting={false}
          onClose={() => undefined}
          onResolve={onResolve}
        />
      </MantineProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "Resolve" }));
    expect(onResolve).not.toHaveBeenCalled();
  });
});
