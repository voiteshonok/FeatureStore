import { Badge, Group, Paper, Stack, Text, Title } from "@mantine/core";
import { useSelector } from "react-redux";
import { Link } from "react-router-dom";

import type { components } from "@shared/api/schema";
import { EmptyState } from "@shared/ui/EmptyState";
import { ListSkeleton } from "@shared/ui/ListSkeleton";

import { formatClock } from "../model/format";
import { selectHighlightedAlertIds, selectRuleNames } from "../model/streamSelectors";
import type { StreamState } from "../model/streamSlice";

import classes from "./RecentAlerts.module.css";

type AlertOut = components["schemas"]["AlertOut"];
type RuleOut = components["schemas"]["RuleOut"];

type RecentAlertsProps = {
  items: AlertOut[];
  rules: RuleOut[];
  loading: boolean;
  error: boolean;
};

export function RecentAlerts({ items, rules, loading, error }: RecentAlertsProps) {
  const ruleNames = useSelector((state: { stream: StreamState }) => selectRuleNames(state));
  const highlighted = useSelector((state: { stream: StreamState }) =>
    selectHighlightedAlertIds(state),
  );

  return (
    <Stack gap="sm">
      <Title order={3}>Recent alerts</Title>
      {loading && items.length === 0 ? <ListSkeleton rows={4} /> : null}
      {!loading && error ? (
        <EmptyState
          title="Could not load alerts"
          description="The list refreshes when the stream reconnects."
        />
      ) : null}
      {!loading && !error && items.length === 0 ? (
        <EmptyState title="No alerts" description="When a rule fires, the alert shows up here." />
      ) : null}
      {items.length > 0 ? (
        <Stack gap="xs">
          {items.map((item) => {
            const isNew = highlighted.includes(item.alert_id);
            return (
              <Paper
                key={item.alert_id}
                component={Link}
                to={`/alerts/${item.alert_id}`}
                withBorder
                p="sm"
                td="none"
                c={isNew ? undefined : "inherit"}
                className={isNew ? classes.fresh : undefined}
              >
                <Group justify="space-between" wrap="wrap" gap="sm">
                  <Stack gap={2} miw="0">
                    <Group gap="xs">
                      <Text fw={600}>{ruleLabel(item.rule_id, ruleNames, rules)}</Text>
                      {isNew ? (
                        <Badge color="yellow" variant="filled">
                          New
                        </Badge>
                      ) : null}
                    </Group>
                    <Text className={classes.token} size="sm" c="dimmed">
                      {entityLabel(item)}
                    </Text>
                  </Stack>
                  <Stack gap={2} align="flex-end">
                    <Text size="sm">{item.status}</Text>
                    <Text size="xs" c="dimmed">
                      {formatClock(item.created_at)}
                    </Text>
                  </Stack>
                </Group>
              </Paper>
            );
          })}
        </Stack>
      ) : null}
    </Stack>
  );
}

function ruleLabel(ruleId: string, streamNames: Record<string, string>, rules: RuleOut[]): string {
  const fromStream = streamNames[ruleId];
  if (fromStream) {
    return fromStream;
  }
  const fromRest = rules.find((rule) => rule.rule_id === ruleId);
  return fromRest?.name ?? ruleId;
}

function entityLabel(item: AlertOut): string {
  if (item.card_token) {
    return `card ${item.card_token}`;
  }
  if (item.merchant_id) {
    return `merchant ${item.merchant_id}`;
  }
  return "—";
}
