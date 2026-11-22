import { describe, expect, it, vi } from "vitest";

import { featureLookupPushed, featureLookupReducer } from "./featureLookupSlice";
import type { LookupFormValues } from "./schema";

function entry(index: number): LookupFormValues {
  return {
    cardToken: `token-${index}`,
    merchantId: "m",
    lat: "",
    lon: "",
    ts: `2026-11-22T12:00:0${index}`,
  };
}

describe("featureLookup slice", () => {
  it("keeps five requests in memory and does not touch localStorage", () => {
    const setItem = vi.spyOn(Storage.prototype, "setItem");
    let state = featureLookupReducer(undefined, { type: "init" });
    for (let index = 0; index < 6; index += 1) {
      state = featureLookupReducer(state, featureLookupPushed(entry(index)));
    }
    expect(state.recent).toHaveLength(5);
    expect(state.recent[0]?.cardToken).toBe("token-5");
    expect(state.recent[4]?.cardToken).toBe("token-1");
    expect(setItem).not.toHaveBeenCalled();
    setItem.mockRestore();
  });
});
