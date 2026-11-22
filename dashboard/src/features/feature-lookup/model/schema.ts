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
  lat: string;
  lon: string;
  ts: string;
};

export function emptyLookupForm(now = Date.now()): LookupFormValues {
  return {
    cardToken: "",
    merchantId: "",
    lat: "",
    lon: "",
    ts: toLocalInput(new Date(now).toISOString()),
  };
}

export function lookupSchema(now = Date.now()) {
  return z
    .object({
      cardToken: z.string().trim(),
      merchantId: z.string().trim(),
      lat: z.string(),
      lon: z.string(),
      ts: z.string().trim(),
    })
    .superRefine((value, ctx) => {
      if (PAN.test(value.cardToken)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["cardToken"],
          message: "A token is required, not a card number",
        });
      } else if (!HEX_64.test(value.cardToken)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["cardToken"],
          message: "Token must be 64 hex characters",
        });
      }
      if (value.merchantId.length === 0) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["merchantId"],
          message: "Merchant id is required",
        });
      } else if (value.merchantId.length > 64) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["merchantId"],
          message: "At most 64 characters",
        });
      }
      const lat = readCoord(value.lat);
      const lon = readCoord(value.lon);
      if (lat === "bad") {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["lat"],
          message: "Latitude must be a number",
        });
      } else if (typeof lat === "number" && (lat < -90 || lat > 90)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["lat"],
          message: "Latitude must be between -90 and 90",
        });
      }
      if (lon === "bad") {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["lon"],
          message: "Longitude must be a number",
        });
      } else if (typeof lon === "number" && (lon < -180 || lon > 180)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["lon"],
          message: "Longitude must be between -180 and 180",
        });
      }
      if (lat !== "bad" && lon !== "bad" && (lat === null) !== (lon === null)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: [lat === null ? "lat" : "lon"],
          message: "Enter both coordinates or neither",
        });
      }
      if (!value.ts) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["ts"],
          message: "Time is required",
        });
        return;
      }
      const ts = new Date(value.ts);
      if (Number.isNaN(ts.getTime())) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["ts"],
          message: "Time is invalid",
        });
        return;
      }
      if (ts.getTime() > now + FUTURE_MS) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["ts"],
          message: "Time must not be more than 5 seconds ahead",
        });
      } else if (ts.getTime() < now - DAY_MS) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["ts"],
          message: "Time must not be older than 24 hours",
        });
      }
    });
}

export function toFeatureRequest(values: LookupFormValues): FeatureRequest {
  const lat = readCoord(values.lat);
  const lon = readCoord(values.lon);
  const body: FeatureRequest = {
    card_token: values.cardToken.trim().toLowerCase(),
    merchant_id: values.merchantId.trim(),
    ts: new Date(values.ts).toISOString(),
  };
  if (typeof lat === "number" && typeof lon === "number") {
    body.lat = lat;
    body.lon = lon;
  }
  return body;
}

export function formFromSearch(params: URLSearchParams, now = Date.now()): LookupFormValues {
  const ts = params.get("ts");
  return {
    cardToken: params.get("card_token") ?? "",
    merchantId: params.get("merchant_id") ?? "",
    lat: params.get("lat") ?? "",
    lon: params.get("lon") ?? "",
    ts: ts ? toLocalInput(ts) : toLocalInput(new Date(now).toISOString()),
  };
}

export function writeLookupSearch(values: LookupFormValues): URLSearchParams {
  const params = new URLSearchParams();
  params.set("card_token", values.cardToken.trim().toLowerCase());
  params.set("merchant_id", values.merchantId.trim());
  if (values.lat.trim()) {
    params.set("lat", values.lat.trim());
  }
  if (values.lon.trim()) {
    params.set("lon", values.lon.trim());
  }
  const ts = new Date(values.ts);
  if (!Number.isNaN(ts.getTime())) {
    params.set("ts", ts.toISOString());
  }
  return params;
}

function readCoord(raw: string): number | null | "bad" {
  const trimmed = raw.trim();
  if (!trimmed) {
    return null;
  }
  const value = Number(trimmed);
  if (!Number.isFinite(value)) {
    return "bad";
  }
  return value;
}

export function toLocalInput(iso: string): string {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}
