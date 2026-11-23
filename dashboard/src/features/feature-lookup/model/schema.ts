import { z } from "zod";

import type { components } from "@shared/api/schema";

type FeatureRequest = components["schemas"]["FeatureRequest"];

const PAN = /^\d{13,19}$/;
const HEX_64 = /^[0-9a-f]{64}$/i;
const DAY_MS = 24 * 60 * 60 * 1000;
const FUTURE_MS = 5_000;

export type LookupFormValues = {
  cardToken: string;
  merchantId: string;
  ts: string;
};

export function emptyLookupForm(now = Date.now()): LookupFormValues {
  return {
    cardToken: "",
    merchantId: "",
    ts: toLocalInput(new Date(now).toISOString()),
  };
}

export function lookupSchema(now = Date.now()) {
  return z
    .object({
      cardToken: z.string().trim(),
      merchantId: z.string().trim(),
      ts: z.string().trim(),
    })
    .superRefine((value, ctx) => {
      if (value.cardToken.length > 0) {
        if (PAN.test(value.cardToken)) {
          ctx.addIssue({
            code: "custom",
            path: ["cardToken"],
            message: "A token is required, not a card number",
          });
        } else if (!HEX_64.test(value.cardToken)) {
          ctx.addIssue({
            code: "custom",
            path: ["cardToken"],
            message: "Token must be 64 hex characters",
          });
        }
      }
      if (value.merchantId.length > 64) {
        ctx.addIssue({
          code: "custom",
          path: ["merchantId"],
          message: "At most 64 characters",
        });
      }
      if (value.cardToken.length === 0 && value.merchantId.length === 0) {
        ctx.addIssue({
          code: "custom",
          path: ["cardToken"],
          message: "Enter a card token or a merchant id",
        });
      }
      if (!value.ts) {
        ctx.addIssue({
          code: "custom",
          path: ["ts"],
          message: "Time is required",
        });
        return;
      }
      const ts = new Date(value.ts);
      if (Number.isNaN(ts.getTime())) {
        ctx.addIssue({
          code: "custom",
          path: ["ts"],
          message: "Time is invalid",
        });
        return;
      }
      if (ts.getTime() > now + FUTURE_MS) {
        ctx.addIssue({
          code: "custom",
          path: ["ts"],
          message: "Time must not be more than 5 seconds ahead",
        });
      } else if (ts.getTime() < now - DAY_MS) {
        ctx.addIssue({
          code: "custom",
          path: ["ts"],
          message: "Time must not be older than 24 hours",
        });
      }
    });
}

export function toFeatureRequest(values: LookupFormValues): FeatureRequest {
  const body: FeatureRequest = {
    ts: new Date(values.ts).toISOString(),
  };
  const token = values.cardToken.trim().toLowerCase();
  const merchant = values.merchantId.trim();
  if (token) {
    body.card_token = token;
  }
  if (merchant) {
    body.merchant_id = merchant;
  }
  return body;
}

export function formFromSearch(params: URLSearchParams, now = Date.now()): LookupFormValues {
  const ts = params.get("ts");
  return {
    cardToken: params.get("card_token") ?? "",
    merchantId: params.get("merchant_id") ?? "",
    ts: ts ? toLocalInput(ts) : toLocalInput(new Date(now).toISOString()),
  };
}

export function writeLookupSearch(values: LookupFormValues): URLSearchParams {
  const params = new URLSearchParams();
  const token = values.cardToken.trim().toLowerCase();
  const merchant = values.merchantId.trim();
  if (token) {
    params.set("card_token", token);
  }
  if (merchant) {
    params.set("merchant_id", merchant);
  }
  const ts = new Date(values.ts);
  if (!Number.isNaN(ts.getTime())) {
    params.set("ts", ts.toISOString());
  }
  return params;
}

export function toLocalInput(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}
