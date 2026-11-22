import { describe, expect, it } from "vitest";

import { featureGroup, formatAge, formatFeatureValue } from "./format";

describe("feature formatting", () => {
  it("turns euro cents, rates, and age into display text", () => {
    expect(formatFeatureValue("sum", 1234)).toBe("12.34 €");
    expect(formatFeatureValue("decline_rate", 0.356)).toBe("35.6%");
    expect(formatFeatureValue("count", 6)).toBe("6");
    expect(formatAge(1200)).toBe("1.2 s");
    expect(formatAge(180_000)).toBe("3 min");
    expect(formatAge(null)).toBe("—");
  });

  it("keeps on-demand features out of the card group", () => {
    expect(featureGroup({ entity: "card", kind: "on_demand" })).toBe("on_demand");
    expect(featureGroup({ entity: "card", kind: "stream" })).toBe("card");
    expect(featureGroup({ entity: "merchant", kind: "stream" })).toBe("merchant");
  });
});
