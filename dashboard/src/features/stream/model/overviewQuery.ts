import type { AlertsListArgs } from "@shared/api/baseApi";

/** First page the overview keeps warm. Stream events patch this cache entry. */
export const OVERVIEW_ALERTS_QUERY = {
  limit: 10,
  sort: "-created_at",
} as const satisfies AlertsListArgs;

export function isOverviewAlertsQuery(args: AlertsListArgs): boolean {
  if (args.limit !== OVERVIEW_ALERTS_QUERY.limit || args.sort !== OVERVIEW_ALERTS_QUERY.sort) {
    return false;
  }
  if (args.cursor) {
    return false;
  }
  if (args.status && args.status.length > 0) {
    return false;
  }
  if (args.entity || args.rule_id || args.created_from || args.created_to) {
    return false;
  }
  return true;
}
