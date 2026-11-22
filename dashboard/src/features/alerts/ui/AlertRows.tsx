import { Badge, Button, Group, Paper, Stack, Table, Text } from "@mantine/core";
import { Link } from "react-router-dom";

import type { components } from "@shared/api/schema";
import { formatClock } from "@features/stream/model/format";

import { changedBy, entityLabel } from "../model/filters";

type AlertOut = components["schemas"]["AlertOut"];

type AlertRowsProps = {
  items: AlertOut[];
  ruleName: (ruleId: string) => string;
  nextCursor: string | null;
  loadingMore: boolean;
  onMore: () => void;
};

export function AlertRows({ items, ruleName, nextCursor, loadingMore, onMore }: AlertRowsProps) {
  return (
    <Stack gap="sm">
      <Table highlightOnHover visibleFrom="sm">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>Time</Table.Th>
            <Table.Th>Rule</Table.Th>
            <Table.Th>Entity</Table.Th>
            <Table.Th>Status</Table.Th>
            <Table.Th visibleFrom="lg">Changed</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {items.map((item) => {
            const changed = changedBy(item);
            return (
              <Table.Tr key={item.alert_id}>
                <Table.Td>
                  <Text component={Link} to={`/alerts/${item.alert_id}`} size="sm">
                    {formatClock(item.created_at)}
                  </Text>
                </Table.Td>
                <Table.Td>{ruleName(item.rule_id)}</Table.Td>
                <Table.Td>{entityLabel(item, true)}</Table.Td>
                <Table.Td>
                  <StatusBadge status={item.status} />
                </Table.Td>
                <Table.Td visibleFrom="lg">
                  {changed ? `${changed.who} · ${formatClock(changed.when)}` : "—"}
                </Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
      <Stack gap="xs" hiddenFrom="sm">
        {items.map((item) => {
          const changed = changedBy(item);
          return (
            <Paper
              key={item.alert_id}
              component={Link}
              to={`/alerts/${item.alert_id}`}
              withBorder
              p="sm"
              td="none"
              c="inherit"
            >
              <Group justify="space-between" wrap="nowrap">
                <Stack gap={2}>
                  <Text fw={600}>{ruleName(item.rule_id)}</Text>
                  <Text size="sm">{entityLabel(item, true)}</Text>
                  <Text size="xs" c="dimmed">
                    {formatClock(item.created_at)}
                    {changed ? ` · ${changed.who} ${formatClock(changed.when)}` : ""}
                  </Text>
                </Stack>
                <StatusBadge status={item.status} />
              </Group>
            </Paper>
          );
        })}
      </Stack>
      {nextCursor ? (
        <Button variant="light" onClick={onMore} loading={loadingMore}>
          Show more
        </Button>
      ) : null}
    </Stack>
  );
}

function StatusBadge({ status }: { status: AlertOut["status"] }) {
  const color = status === "open" ? "red" : status === "acknowledged" ? "yellow" : "green";
  return (
    <Badge color={color} variant="light">
      {status}
    </Badge>
  );
}
