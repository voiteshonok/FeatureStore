import { describe, expect, it } from "vitest";

import { acceptAlertsEvent, acceptSystemEvent } from "./streamEvents";

const alert = {
  id: 1,
  op: "created",
  alert_id: "11111111-1111-4111-8111-111111111111",
  rule_id: "22222222-2222-4222-8222-222222222222",
  rule_name: "card_velocity_burst",
  status: "open",
  entity: "card",
  entity_id: "abcd…ef",
  created_at: "2026-11-22T12:00:00.000Z",
};

describe("acceptAlertsEvent", () => {
  it("accepts a valid alert and an empty reset", () => {
    expect(acceptAlertsEvent("alert", alert).kind).toBe("alert");
    expect(acceptAlertsEvent("reset", {}).kind).toBe("reset");
  });

  it("rejects a malformed alert without dropping the kind", () => {
    expect(acceptAlertsEvent("alert", { op: "created" }).kind).toBe("invalid");
    expect(acceptAlertsEvent("stats", { events_per_s: 1 }).kind).toBe("invalid");
  });

  it("ignores unknown event names", () => {
    expect(acceptAlertsEvent("hello", {}).kind).toBe("ignore");
  });
});

describe("acceptSystemEvent", () => {
  it("accepts fallback mode", () => {
    const parsed = acceptSystemEvent("change", {
      mode: "fallback",
      components: [
        {
          name: "Redis",
          status: "down",
          checked_at: "2026-11-22T12:00:00.000Z",
          lag: null,
        },
      ],
    });
    expect(parsed.kind).toBe("snapshot");
  });
});
