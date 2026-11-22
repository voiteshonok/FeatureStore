import { memo, type ReactNode } from "react";

import { Box, Grid, Stack, Text, Title } from "@mantine/core";
import { useSelector } from "react-redux";

import { OVERVIEW_ALERTS_QUERY } from "@features/stream/model/overviewQuery";
import { selectLatestStats, selectStaleSince } from "@features/stream/model/streamSelectors";
import type { StreamState } from "@features/stream/model/streamSlice";
import { AlertsByRuleChart } from "@features/stream/ui/AlertsByRuleChart";
import { EventsChart } from "@features/stream/ui/EventsChart";
import { RecentAlerts } from "@features/stream/ui/RecentAlerts";
import { StatTiles } from "@features/stream/ui/StatTiles";
import { useNow } from "@features/stream/ui/useNow";
import { SystemGrid } from "@features/system/ui/SystemGrid";
import { useGetAlertsQuery, useGetRulesQuery } from "@shared/api/baseApi";
import type { components } from "@shared/api/schema";
import { OverviewSkeleton } from "@shared/ui/OverviewSkeleton";

import "@mantine/charts/styles.css";

type AlertOut = components["schemas"]["AlertOut"];
type RuleOut = components["schemas"]["RuleOut"];

export default function OverviewPage() {
  const alerts = useGetAlertsQuery(OVERVIEW_ALERTS_QUERY);
  const rules = useGetRulesQuery(undefined);
  const latestAt = useSelector(
    (state: { stream: StreamState }) => selectLatestStats(state)?.receivedAt ?? null,
  );
  const waiting = alerts.isLoading && latestAt == null;

  return (
    <Stack gap="md">
      <div>
        <Title order={2}>Overview</Title>
        <Text c="dimmed" mt="xs">
          Live throughput, alerts, and system status.
        </Text>
      </div>
      {waiting ? (
        <OverviewSkeleton />
      ) : (
        <DimLive>
          <OverviewPanels
            items={alerts.data?.items ?? []}
            rules={rules.data ?? []}
            loading={alerts.isFetching && alerts.data == null}
            error={alerts.isError}
          />
        </DimLive>
      )}
    </Stack>
  );
}

function DimLive({ children }: { children: ReactNode }) {
  const now = useNow(1_000);
  const stale = useSelector(
    (state: { stream: StreamState }) => selectStaleSince(state, now) != null,
  );
  return <Box opacity={stale ? 0.45 : 1}>{children}</Box>;
}

const OverviewPanels = memo(function OverviewPanels({
  items,
  rules,
  loading,
  error,
}: {
  items: AlertOut[];
  rules: RuleOut[];
  loading: boolean;
  error: boolean;
}) {
  return (
    <Stack gap="md">
      <StatTiles />
      <Grid>
        <Grid.Col span={{ base: 12, lg: 8 }}>
          <EventsChart />
        </Grid.Col>
        <Grid.Col span={{ base: 12, sm: 6, lg: 4 }}>
          <AlertsByRuleChart />
        </Grid.Col>
        <Grid.Col span={{ base: 12, sm: 6 }}>
          <SystemGrid />
        </Grid.Col>
        <Grid.Col span={{ base: 12, lg: 6 }}>
          <RecentAlerts items={items} rules={rules} loading={loading} error={error} />
        </Grid.Col>
      </Grid>
    </Stack>
  );
});
