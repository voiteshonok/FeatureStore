/** Alert entity shape used by list/detail screens (filled in T8.4). */
export type AlertStatus = "open" | "acknowledged" | "resolved";
export type AlertEntity = "card" | "merchant";

export type AlertSummary = {
  alertId: string;
  ruleId: string;
  ruleName: string;
  status: AlertStatus;
  entity: AlertEntity;
  entityId: string;
  createdAt: string;
};
