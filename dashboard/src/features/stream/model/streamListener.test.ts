import { describe, expect, it } from "vitest";

import { sessionReducer, sessionResolved, type SessionState } from "@features/session";

import { sessionBecameAuthenticated } from "./streamListener";

describe("sessionBecameAuthenticated", () => {
  it("is true only on the transition into a session", () => {
    const anonymous = sessionReducer(undefined, { type: "init" });
    const authenticated = sessionReducer(
      anonymous,
      sessionResolved({ operatorId: "1", email: "a@x", role: "viewer" }),
    );
    const previous = { session: anonymous };
    const current = { session: authenticated };
    expect(sessionBecameAuthenticated(previous, current)).toBe(true);
    expect(sessionBecameAuthenticated(current, current)).toBe(false);
    const cleared: SessionState = { ...authenticated, status: "anonymous" };
    expect(sessionBecameAuthenticated(current, { session: cleared })).toBe(false);
  });
});
