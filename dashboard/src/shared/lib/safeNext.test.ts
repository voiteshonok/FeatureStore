import { describe, expect, it } from "vitest";

import { safeNextPath } from "./safeNext";

describe("safeNextPath", () => {
  it("allows internal paths", () => {
    expect(safeNextPath("/alerts")).toBe("/alerts");
    expect(safeNextPath("/alerts/abc?x=1")).toBe("/alerts/abc?x=1");
  });

  it("rejects open redirects", () => {
    expect(safeNextPath("//evil.example")).toBe("/");
    expect(safeNextPath("https://evil.example")).toBe("/");
    expect(safeNextPath("http://evil.example")).toBe("/");
    expect(safeNextPath(null)).toBe("/");
  });
});
