import { describe, expect, it } from "vitest";

import { reconnectDelayMs } from "./reconnectDelay";

describe("reconnectDelayMs", () => {
  it("doubles from 1s and caps at 30s", () => {
    expect(reconnectDelayMs(1, () => 0)).toBe(500);
    expect(reconnectDelayMs(1, () => 1)).toBe(1_500);
    expect(reconnectDelayMs(2, () => 1)).toBe(3_000);
    expect(reconnectDelayMs(3, () => 1)).toBe(6_000);
    expect(reconnectDelayMs(6, () => 1)).toBe(30_000);
    expect(reconnectDelayMs(10, () => 0)).toBe(15_000);
  });
});
