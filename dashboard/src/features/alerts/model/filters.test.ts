import { describe, expect, it } from "vitest";

import type { AlertStreamEvent } from "@shared/api/stream";

import {
  filtersActive,
  maskCardToken,
  matchesCreatedAlert,
  parseAlertsSearch,
  periodWindow,
  writeAlertsSearch,
  type AlertsFilter,
} from "./filters";

const base: AlertsFilter = {
  statuses: [],
  ruleId: null,
  entityType: null,
  preset: null,
  from: null,
  to: null,
  sort: "-created_at",
  q: "",
};

function created(overrides: Partial<AlertStreamEvent> = {}): AlertStreamEvent {
  return {
    id: 1,
    op: "created",
    alert_id: "11111111-1111-4111-8111-111111111111",
    rule_id: "22222222-2222-4222-8222-222222222222",
    rule_name: "card_velocity_burst",
    status: "open",
    entity: "card",
    entity_id: "abcd…ef",
    created_at: "2026-11-22T12:00:00.000Z",
    ...overrides,
  };
}

describe("alert filters", () => {
  it("round-trips the query string", () => {
    const filter: AlertsFilter = {
      ...base,
      statuses: ["open", "acknowledged"],
      ruleId: "22222222-2222-4222-8222-222222222222",
      entityType: "card",
      preset: "custom",
      from: "2026-11-22T10:00:00.000Z",
      to: "2026-11-22T12:00:00.000Z",
      sort: "created_at",
      q: "abcd",
    };
    const parsed = parseAlertsSearch(writeAlertsSearch(filter));
    expect(parsed).toEqual(filter);
  });

  it("masks card tokens the same way as the stream", () => {
    expect(maskCardToken("abcdefghijklmnop")).toBe("abcd…op");
    expect(maskCardToken("short")).toBe("…");
  });

  it("offers new alerts only for newest-first rows that match", () => {
    const now = Date.parse("2026-11-22T12:00:00.000Z");
    const window = periodWindow({ ...base, preset: "15m" }, now);
    expect(matchesCreatedAlert(created(), { ...base, preset: "15m" }, window)).toBe(true);
    expect(
      matchesCreatedAlert(created(), { ...base, preset: "15m", sort: "created_at" }, window),
    ).toBe(false);
    expect(
      matchesCreatedAlert(created(), { ...base, preset: "15m", entityType: "merchant" }, window),
    ).toBe(false);
    expect(filtersActive(base)).toBe(false);
    expect(filtersActive({ ...base, q: "ab" })).toBe(true);
  });
});
