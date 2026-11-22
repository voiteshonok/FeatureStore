import type { AlertsListArgs } from "@shared/api/baseApi";
import type { AlertStreamEvent } from "@shared/api/stream";
import type { components } from "@shared/api/schema";

type AlertOut = components["schemas"]["AlertOut"];

export type AlertStatus = AlertOut["status"];
export type EntityType = "card" | "merchant";
export type PeriodPreset = "15m" | "1h" | "24h" | "custom";
export type AlertSort = "created_at" | "-created_at";

export type AlertsFilter = {
  statuses: AlertStatus[];
  ruleId: string | null;
  entityType: EntityType | null;
  preset: PeriodPreset | null;
  from: string | null;
  to: string | null;
  sort: AlertSort;
  q: string;
};

export type PeriodWindow = {
  from: string | null;
  to: string | null;
};

const STATUSES: readonly AlertStatus[] = ["open", "acknowledged", "resolved"];
const PRESETS: readonly PeriodPreset[] = ["15m", "1h", "24h", "custom"];
const PAGE_SIZE = 50;

export const EMPTY_ALERTS_FILTER: AlertsFilter = {
  statuses: [],
  ruleId: null,
  entityType: null,
  preset: null,
  from: null,
  to: null,
  sort: "-created_at",
  q: "",
};

export function parseAlertsSearch(params: URLSearchParams): AlertsFilter {
  const statuses = params
    .getAll("status")
    .filter((value): value is AlertStatus => STATUSES.includes(value as AlertStatus));
  const presetRaw = params.get("preset");
  const preset = PRESETS.includes(presetRaw as PeriodPreset) ? (presetRaw as PeriodPreset) : null;
  const entityRaw = params.get("entity_type");
  const entityType = entityRaw === "card" || entityRaw === "merchant" ? entityRaw : null;
  const sort = params.get("sort") === "created_at" ? "created_at" : "-created_at";
  return {
    statuses,
    ruleId: params.get("rule") || null,
    entityType,
    preset,
    from: params.get("from"),
    to: params.get("to"),
    sort,
    q: params.get("q") ?? "",
  };
}

export function writeAlertsSearch(filter: AlertsFilter): URLSearchParams {
  const params = new URLSearchParams();
  for (const status of filter.statuses) {
    params.append("status", status);
  }
  if (filter.ruleId) {
    params.set("rule", filter.ruleId);
  }
  if (filter.entityType) {
    params.set("entity_type", filter.entityType);
  }
  if (filter.preset) {
    params.set("preset", filter.preset);
  }
  if (filter.preset === "custom" && filter.from) {
    params.set("from", filter.from);
  }
  if (filter.preset === "custom" && filter.to) {
    params.set("to", filter.to);
  }
  if (filter.sort === "created_at") {
    params.set("sort", filter.sort);
  }
  if (filter.q.trim()) {
    params.set("q", filter.q);
  }
  return params;
}

export function serverFilterKey(filter: AlertsFilter): string {
  return JSON.stringify({
    statuses: filter.statuses,
    ruleId: filter.ruleId,
    entityType: filter.entityType,
    preset: filter.preset,
    from: filter.preset === "custom" ? filter.from : null,
    to: filter.preset === "custom" ? filter.to : null,
    sort: filter.sort,
  });
}

export function filtersActive(filter: AlertsFilter): boolean {
  return (
    filter.statuses.length > 0 ||
    filter.ruleId != null ||
    filter.entityType != null ||
    filter.preset != null ||
    filter.q.trim().length > 0
  );
}

export function periodWindow(filter: AlertsFilter, now: number): PeriodWindow {
  if (filter.preset === "15m" || filter.preset === "1h" || filter.preset === "24h") {
    const span =
      filter.preset === "15m" ? 15 * 60_000 : filter.preset === "1h" ? 3_600_000 : 86_400_000;
    return { from: new Date(now - span).toISOString(), to: new Date(now).toISOString() };
  }
  if (filter.preset === "custom") {
    return { from: filter.from, to: filter.to };
  }
  return { from: null, to: null };
}

export function toListArgs(
  filter: AlertsFilter,
  cursor: string | undefined,
  window: PeriodWindow,
): AlertsListArgs {
  return {
    limit: PAGE_SIZE,
    sort: filter.sort,
    status: filter.statuses.length > 0 ? filter.statuses : undefined,
    rule_id: filter.ruleId ?? undefined,
    entity_type: filter.entityType ?? undefined,
    created_from: window.from ?? undefined,
    created_to: window.to ?? undefined,
    cursor,
  };
}

export function maskCardToken(token: string): string {
  if (token.length <= 6) {
    return "…";
  }
  return `${token.slice(0, 4)}…${token.slice(-2)}`;
}

export function entityLabel(
  item: Pick<AlertOut, "card_token" | "merchant_id">,
  masked: boolean,
): string {
  if (item.card_token) {
    return masked ? maskCardToken(item.card_token) : item.card_token;
  }
  if (item.merchant_id) {
    return item.merchant_id;
  }
  return "—";
}

export function matchesSearch(
  item: Pick<AlertOut, "card_token" | "merchant_id">,
  query: string,
): boolean {
  const needle = query.trim().toLowerCase();
  if (!needle) {
    return true;
  }
  const token = item.card_token?.toLowerCase() ?? "";
  const merchant = item.merchant_id?.toLowerCase() ?? "";
  return token.startsWith(needle) || merchant.startsWith(needle);
}

export function matchesCreatedAlert(
  event: AlertStreamEvent,
  filter: AlertsFilter,
  window: PeriodWindow,
): boolean {
  if (filter.sort !== "-created_at") {
    return false;
  }
  if (filter.statuses.length > 0 && !filter.statuses.includes(event.status)) {
    return false;
  }
  if (filter.ruleId && event.rule_id !== filter.ruleId) {
    return false;
  }
  if (filter.entityType && event.entity !== filter.entityType) {
    return false;
  }
  if (window.from && event.created_at < window.from) {
    return false;
  }
  if (window.to && event.created_at > window.to) {
    return false;
  }
  const needle = filter.q.trim().toLowerCase();
  if (needle && !event.entity_id.toLowerCase().startsWith(needle)) {
    return false;
  }
  return true;
}

export function changedBy(item: AlertOut): { who: string; when: string } | null {
  if (item.status === "resolved" && (item.resolved_by || item.resolved_at)) {
    return { who: item.resolved_by ?? "—", when: item.resolved_at ?? "" };
  }
  if (item.status === "acknowledged" && (item.acknowledged_by || item.acknowledged_at)) {
    return { who: item.acknowledged_by ?? "—", when: item.acknowledged_at ?? "" };
  }
  return null;
}
