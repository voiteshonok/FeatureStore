import { describe, expect, it } from "vitest";

import { componentStatusCopy, systemModeCopy } from "./statusCopy";

describe("system status copy", () => {
  it("names degraded and fallback, and keeps normal quiet", () => {
    expect(systemModeCopy("degraded")).toBe("Degraded mode");
    expect(systemModeCopy("fallback")).toBe("Fallback mode");
    expect(systemModeCopy("normal")).toBeNull();
  });

  it("labels status with text", () => {
    expect(componentStatusCopy("up")).toBe("Up");
    expect(componentStatusCopy("down")).toBe("Down");
    expect(componentStatusCopy("unknown")).toBe("Unknown");
  });
});
