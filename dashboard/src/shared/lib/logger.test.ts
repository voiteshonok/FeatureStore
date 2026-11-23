import { afterEach, describe, expect, it, vi } from "vitest";

describe("logger", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.resetModules();
  });

  it("exposes level methods", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { logger } = await import("./logger");
    logger.warn("safe message");
    expect(warn).toHaveBeenCalled();
  });
});
