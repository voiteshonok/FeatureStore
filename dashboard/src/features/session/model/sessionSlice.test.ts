import { describe, expect, it } from "vitest";

import {
  selectCanChangeAlerts,
  sessionResolved,
  sessionReducer,
  type SessionState,
} from "./sessionSlice";

describe("selectCanChangeAlerts", () => {
  it("allows analyst and admin only", () => {
    const viewer: SessionState = sessionReducer(
      undefined,
      sessionResolved({
        operatorId: "1",
        email: "v@x",
        role: "viewer",
      }),
    );
    const analyst: SessionState = sessionReducer(
      undefined,
      sessionResolved({
        operatorId: "2",
        email: "a@x",
        role: "analyst",
      }),
    );
    expect(selectCanChangeAlerts({ session: viewer })).toBe(false);
    expect(selectCanChangeAlerts({ session: analyst })).toBe(true);
  });
});
