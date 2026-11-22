import { useState } from "react";
import { Badge, Button, Group, Stack, Table, Text, Title } from "@mantine/core";
import { Link, useParams } from "react-router-dom";

import { selectCanChangeAlerts, type SessionState } from "@features/session";
import { ResolveDialog } from "@features/alerts/ui/ResolveDialog";
import { entityLabel } from "@features/alerts/model/filters";
import { formatCondition } from "@features/alerts/model/note";
import { formatClock } from "@features/stream/model/format";
import { useGetAlertQuery, usePatchAlertMutation } from "@shared/api/baseApi";
import type { ApiFailure } from "@shared/api/problem";
import { notify } from "@shared/lib/notify";
import { CardSkeleton } from "@shared/ui/CardSkeleton";
import { EmptyState } from "@shared/ui/EmptyState";
import { useSelector } from "react-redux";

function asFailure(error: unknown): ApiFailure | null {
  if (!error || typeof error !== "object" || !("status" in error)) {
    return null;
  }
  return error as ApiFailure;
}

export default function AlertDetailPage() {
  const { alertId = "" } = useParams();
  const { data, error, isLoading, isError } = useGetAlertQuery(alertId, { skip: !alertId });
  const [patchAlert, { isLoading: patching }] = usePatchAlertMutation();
  const canChange = useSelector((state: { session: SessionState }) => selectCanChangeAlerts(state));
  const [resolveOpen, setResolveOpen] = useState(false);
  const [baselineStatus, setBaselineStatus] = useState<string | null>(null);
  const failure = asFailure(error);

  if (!alertId) {
    return (
      <EmptyState
        title="Alert not found"
        description="Missing alert id."
        actionLabel="Back to alerts"
        actionTo="/alerts"
      />
    );
  }
  if (isLoading) {
    return <CardSkeleton />;
  }
  if (isError && failure?.status === 404) {
    return (
      <EmptyState
        title="Alert not found"
        description={`No alert ${alertId}.`}
        actionLabel="Back to alerts"
        actionTo="/alerts"
      />
    );
  }
  if (isError && failure?.status === 403) {
    return (
      <EmptyState
        title="Access denied"
        description="You do not have permission to view this alert."
        actionLabel="Back to overview"
        actionTo="/"
      />
    );
  }
  if (isError || !data) {
    return (
      <EmptyState
        title="Could not load"
        description="Try refreshing the page."
        actionLabel="Back to alerts"
        actionTo="/alerts"
      />
    );
  }

  const { alert, rule, history } = data;
  const subject = alert.card_token ?? alert.merchant_id ?? "";
  const canTake = canChange && alert.status === "open";
  const canResolve = canChange && (alert.status === "open" || alert.status === "acknowledged");
  const featureQuery = new URLSearchParams();
  if (alert.card_token) {
    featureQuery.set("card_token", alert.card_token);
  }
  if (alert.merchant_id) {
    featureQuery.set("merchant_id", alert.merchant_id);
  }

  return (
    <Stack gap="md">
      <Group justify="space-between" align="flex-start">
        <div>
          <Title order={2}>{rule.name}</Title>
          <Text size="sm" c="dimmed" mt={4}>
            {alert.status} · {formatClock(alert.created_at)}
          </Text>
        </div>
        {canChange ? (
          <Group gap="xs">
            {canTake ? (
              <Button
                variant="light"
                loading={patching}
                onClick={() => {
                  void patchAlert({ alertId, body: { status: "acknowledged" } });
                }}
              >
                Take
              </Button>
            ) : null}
            {canResolve ? (
              <Button
                loading={patching}
                onClick={() => {
                  setBaselineStatus(alert.status);
                  setResolveOpen(true);
                }}
              >
                Resolve
              </Button>
            ) : null}
          </Group>
        ) : null}
      </Group>
      <Group gap="xs">
        <Text ff="monospace" size="sm">
          {entityLabel(alert, false)}
        </Text>
        {subject ? (
          <Button
            size="compact-sm"
            variant="default"
            aria-label="Copy"
            onClick={() => {
              void navigator.clipboard.writeText(subject).then(
                () => {
                  notify("Copied", "success");
                },
                () => {
                  notify("Could not copy");
                },
              );
            }}
          >
            Copy
          </Button>
        ) : null}
        <Button
          component={Link}
          to={`/features?${featureQuery.toString()}`}
          variant="subtle"
          size="compact-sm"
        >
          Current features
        </Button>
      </Group>

      <div>
        <Title order={3}>Why it fired</Title>
        <Table mt="sm">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>Condition</Table.Th>
              <Table.Th>Value</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rule.conditions.map((condition) => (
              <Table.Tr key={condition.feature_name}>
                <Table.Td>{formatCondition(condition)}</Table.Td>
                <Table.Td>{alert.feature_snapshot[condition.feature_name] ?? "—"}</Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </div>

      <div>
        <Title order={3}>History</Title>
        <Stack gap="xs" mt="sm">
          {history.length === 0 ? <Text c="dimmed">No status changes yet.</Text> : null}
          {history.map((entry) => {
            const detail = readDetail(entry.detail);
            return (
              <Group key={entry.audit_id} justify="space-between" align="flex-start">
                <Stack gap={2}>
                  <Text size="sm">
                    {entry.actor ?? "unknown"} · {detail.from ?? "?"} → {detail.to ?? entry.action}
                  </Text>
                  {detail.note ? (
                    <Text size="sm" c="dimmed">
                      {detail.note}
                    </Text>
                  ) : null}
                </Stack>
                <Badge variant="light">{formatClock(entry.occurred_at)}</Badge>
              </Group>
            );
          })}
        </Stack>
      </div>

      <ResolveDialog
        opened={resolveOpen}
        liveStatus={alert.status}
        baselineStatus={baselineStatus}
        submitting={patching}
        onClose={() => {
          setResolveOpen(false);
        }}
        onResolve={(note) => {
          void patchAlert({ alertId, body: { status: "resolved", note } }).then((result) => {
            if ("data" in result) {
              setResolveOpen(false);
            }
          });
        }}
      />
    </Stack>
  );
}

function readDetail(detail: object): {
  from: string | null;
  to: string | null;
  note: string | null;
} {
  const row = detail as Record<string, unknown>;
  return {
    from: typeof row.from === "string" ? row.from : null,
    to: typeof row.to === "string" ? row.to : null,
    note: typeof row.resolution_note === "string" ? row.resolution_note : null,
  };
}
