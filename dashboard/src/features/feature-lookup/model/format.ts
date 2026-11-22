import type { components } from "@shared/api/schema";

type CatalogEntry = components["schemas"]["CatalogEntry"];
type FeatureStatus = components["schemas"]["FeatureStatus"];

export type FeatureGroup = "card" | "merchant" | "on_demand";

export function featureGroup(entry: Pick<CatalogEntry, "entity" | "kind">): FeatureGroup {
  if (entry.kind === "on_demand") {
    return "on_demand";
  }
  if (entry.entity === "merchant") {
    return "merchant";
  }
  return "card";
}

export function formatFeatureValue(agg: string, value: number | null | undefined): string {
  if (value == null) {
    return "—";
  }
  if (agg === "sum") {
    return `${(value / 100).toFixed(2)} €`;
  }
  if (agg === "decline_rate") {
    return `${trimNumber(value * 100)}%`;
  }
  if (agg === "geo_velocity") {
    return `${trimNumber(value)} km/h`;
  }
  return String(Math.round(value));
}

export function formatAge(ageMs: number | null | undefined): string {
  if (ageMs == null) {
    return "—";
  }
  if (ageMs < 60_000) {
    return `${trimNumber(ageMs / 1000)} s`;
  }
  return `${trimNumber(ageMs / 60_000)} min`;
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null) {
    return "—";
  }
  if (ms % 86_400_000 === 0) {
    return `${ms / 86_400_000} h`;
  }
  if (ms % 3_600_000 === 0) {
    return `${ms / 3_600_000} h`;
  }
  if (ms % 60_000 === 0) {
    return `${ms / 60_000} min`;
  }
  if (ms % 1000 === 0) {
    return `${ms / 1000} s`;
  }
  return `${ms} ms`;
}

export const STATUS_HELP: Record<FeatureStatus, string> = {
  fresh: "Age is within the stale threshold.",
  stale: "Age is past the stale threshold.",
  default: "No data for this entity yet; the catalog default is shown.",
  fallback: "Reserved for stage 5, when Redis is unavailable.",
};

function trimNumber(value: number): string {
  if (Number.isInteger(value)) {
    return String(value);
  }
  return value.toFixed(1);
}
