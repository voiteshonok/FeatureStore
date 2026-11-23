import { Badge, Paper, Stack, Table, Text, Title, Tooltip } from "@mantine/core";
import { IconAlertTriangle, IconCircleCheck, IconClock, IconMinus } from "@tabler/icons-react";

import type { components } from "@shared/api/schema";
import { formatClock } from "@features/stream/model/format";

import {
  STATUS_HELP,
  featureGroup,
  formatAge,
  formatDuration,
  formatFeatureValue,
  type FeatureGroup,
} from "../model/format";

type CatalogEntry = components["schemas"]["CatalogEntry"];
type FeatureResponse = components["schemas"]["FeatureResponse"];
type FeatureStatus = components["schemas"]["FeatureStatus"];

const GROUPS: { id: FeatureGroup; title: string }[] = [
  { id: "card", title: "Card" },
  { id: "merchant", title: "Merchant" },
  { id: "on_demand", title: "On demand" },
];

const STATUS_COLOR: Record<FeatureStatus, string> = {
  fresh: "green",
  stale: "yellow",
  default: "gray",
  fallback: "orange",
};

type FeatureTableProps = {
  catalog: CatalogEntry[];
  response: FeatureResponse | null;
  elapsedMs: number | null;
};

export function FeatureTable({ catalog, response, elapsedMs }: FeatureTableProps) {
  return (
    <Stack gap="md">
      {response ? (
        <Text size="sm" c="dimmed">
          request_id {response.request_id}
          {elapsedMs != null ? ` · ${elapsedMs} ms` : ""}
        </Text>
      ) : (
        <Text size="sm" c="dimmed">
          Look up a card, a merchant, or both.
        </Text>
      )}
      {response
        ? GROUPS.map((group) => {
            const rows = catalog.filter(
              (entry) => featureGroup(entry) === group.id && entry.name in response.features,
            );
            if (rows.length === 0) {
              return null;
            }
            return (
              <div key={group.id}>
                <Title order={3}>{group.title}</Title>
                <Stack gap="xs" mt="sm" hiddenFrom="sm">
                  {rows.map((entry) => {
                    const feature = response.features[entry.name];
                    return (
                      <Paper key={entry.name} withBorder p="sm">
                        <Stack gap={4}>
                          <Text fw={600}>{entry.name}</Text>
                          <Text size="sm">
                            {entry.entity} · {formatDuration(entry.window_ms)}
                          </Text>
                          <Text size="sm">{formatFeatureValue(entry.agg, feature?.value)}</Text>
                          {feature ? (
                            <StatusBadge status={feature.status} />
                          ) : (
                            <Text size="sm">—</Text>
                          )}
                        </Stack>
                      </Paper>
                    );
                  })}
                </Stack>
                <Table mt="sm" visibleFrom="sm">
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th>Feature</Table.Th>
                      <Table.Th>Entity</Table.Th>
                      <Table.Th>Window</Table.Th>
                      <Table.Th>Value</Table.Th>
                      <Table.Th>Status</Table.Th>
                      <Table.Th visibleFrom="lg">As of</Table.Th>
                      <Table.Th visibleFrom="lg">Age</Table.Th>
                      <Table.Th visibleFrom="lg">Stale after</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {rows.map((entry) => {
                      const feature = response.features[entry.name];
                      return (
                        <Table.Tr key={entry.name}>
                          <Table.Td>{entry.name}</Table.Td>
                          <Table.Td>{entry.entity}</Table.Td>
                          <Table.Td>{formatDuration(entry.window_ms)}</Table.Td>
                          <Table.Td>{formatFeatureValue(entry.agg, feature?.value)}</Table.Td>
                          <Table.Td>
                            {feature ? <StatusBadge status={feature.status} /> : "—"}
                          </Table.Td>
                          <Table.Td visibleFrom="lg">
                            {feature?.as_of ? formatClock(feature.as_of) : "—"}
                          </Table.Td>
                          <Table.Td visibleFrom="lg">{formatAge(feature?.age_ms)}</Table.Td>
                          <Table.Td visibleFrom="lg">
                            {formatDuration(entry.stale_after_ms)}
                          </Table.Td>
                        </Table.Tr>
                      );
                    })}
                  </Table.Tbody>
                </Table>
              </div>
            );
          })
        : null}
    </Stack>
  );
}

function StatusBadge({ status }: { status: FeatureStatus }) {
  return (
    <Tooltip label={STATUS_HELP[status]} multiline maw={240}>
      <Badge
        color={STATUS_COLOR[status]}
        variant="light"
        leftSection={<StatusIcon status={status} />}
      >
        {status}
      </Badge>
    </Tooltip>
  );
}

function StatusIcon({ status }: { status: FeatureStatus }) {
  const props = { size: 14, "aria-hidden": true } as const;
  if (status === "fresh") {
    return <IconCircleCheck {...props} />;
  }
  if (status === "stale") {
    return <IconClock {...props} />;
  }
  if (status === "fallback") {
    return <IconAlertTriangle {...props} />;
  }
  return <IconMinus {...props} />;
}
