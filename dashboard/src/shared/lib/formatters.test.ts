import { describe, expect, it } from "vitest";

import { formatCount, formatDateTime } from "./formatters";

describe("formatters", () => {
  it("formats a timestamp and a count", () => {
    expect(formatDateTime("2026-11-22T12:00:00.000Z")).toBe("2026-11-22 12:00:00 UTC");
    expect(formatDateTime("not-a-date")).toBe("—");
    expect(formatCount(12.4)).toBe("12.4");
    expect(formatCount(Number.NaN)).toBe("—");
  });
});
