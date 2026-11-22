import { describe, expect, it } from "vitest";

import { noteError, resolutionNote } from "./note";

describe("resolution note", () => {
  it("builds a note and rejects comments that miss the length bounds", () => {
    expect(resolutionNote("false_positive", "looks fine")).toBe("False positive: looks fine");
    expect(noteError("no")).toBe("Comment must be at least 3 characters");
    expect(noteError(resolutionNote("confirmed_fraud", "x".repeat(490)))).toMatch(/500/);
  });
});
