import { describe, expect, it } from "vitest";

import { lookupSchema, toFeatureRequest } from "./schema";

const NOW = Date.parse("2026-11-22T12:00:00.000Z");
const TOKEN = "ab".repeat(32);

function localInput(ms: number): string {
  const date = new Date(ms);
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function values(
  overrides: Partial<{
    cardToken: string;
    merchantId: string;
    ts: string;
  }> = {},
) {
  return {
    cardToken: TOKEN,
    merchantId: "merchant-1",
    ts: localInput(NOW),
    ...overrides,
  };
}

describe("lookupSchema", () => {
  it("rejects a card number and a short token", () => {
    const pan = lookupSchema(NOW).safeParse(values({ cardToken: "4111111111111111" }));
    expect(pan.success).toBe(false);
    if (!pan.success) {
      expect(pan.error.issues.some((issue) => issue.message.includes("not a card number"))).toBe(
        true,
      );
    }
    const short = lookupSchema(NOW).safeParse(values({ cardToken: "abcd" }));
    expect(short.success).toBe(false);
  });

  it("accepts either identifier and rejects both empty", () => {
    expect(lookupSchema(NOW).safeParse(values({ merchantId: "" })).success).toBe(true);
    expect(lookupSchema(NOW).safeParse(values({ cardToken: "" })).success).toBe(true);
    const neither = lookupSchema(NOW).safeParse(values({ cardToken: "", merchantId: "" }));
    expect(neither.success).toBe(false);
    if (!neither.success) {
      expect(
        neither.error.issues.some(
          (issue) => issue.message === "Enter a card token or a merchant id",
        ),
      ).toBe(true);
    }
  });

  it("rejects a timestamp outside the 24 hour window", () => {
    const old = lookupSchema(NOW).safeParse(
      values({ ts: localInput(NOW - 24 * 60 * 60 * 1000 - 1000) }),
    );
    expect(old.success).toBe(false);
    const ahead = lookupSchema(NOW).safeParse(values({ ts: localInput(NOW + 6_000) }));
    expect(ahead.success).toBe(false);
  });

  it("sends only the identifiers that were filled", () => {
    const both = toFeatureRequest(values());
    expect(both.card_token).toBe(TOKEN);
    expect(both.merchant_id).toBe("merchant-1");
    expect(both.lat).toBeUndefined();
    expect(both.lon).toBeUndefined();

    const card = toFeatureRequest(values({ merchantId: "" }));
    expect(card.card_token).toBe(TOKEN);
    expect(card.merchant_id).toBeUndefined();

    const merchant = toFeatureRequest(values({ cardToken: "" }));
    expect(merchant.card_token).toBeUndefined();
    expect(merchant.merchant_id).toBe("merchant-1");
  });
});
