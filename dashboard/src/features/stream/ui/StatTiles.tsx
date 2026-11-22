import { Paper, SimpleGrid, Stack, Text } from "@mantine/core";
import { Sparkline } from "@mantine/charts";
import { useSelector } from "react-redux";

import { alertsInWindow, formatCount, formatLatency, formatRate } from "../model/format";
import { selectLatestStats, selectStatSparklines } from "../model/streamSelectors";
import type { StreamState } from "../model/streamSlice";

export function StatTiles() {
  const latest = useSelector((state: { stream: StreamState }) => selectLatestStats(state));
  const sparks = useSelector((state: { stream: StreamState }) => selectStatSparklines(state));
  const alerts60s = latest ? alertsInWindow(latest.alerts_60s_by_rule) : null;

  return (
    <SimpleGrid cols={{ base: 1, sm: 2, md: 4 }}>
      <Tile
        label="Events/s"
        value={latest ? formatRate(latest.events_per_s) : "—"}
        series={sparks.eventsPerS}
      />
      <Tile
        label="Alerts (60s)"
        value={alerts60s == null ? "—" : formatCount(alerts60s)}
        series={sparks.alerts60s}
      />
      <Tile
        label="p95 latency"
        value={formatLatency(latest?.latency_p95_ms)}
        series={sparks.latencyP95}
      />
      <Tile
        label="Active cards"
        value={latest ? formatCount(latest.active_cards_60s) : "—"}
        series={sparks.activeCards}
      />
    </SimpleGrid>
  );
}

function Tile({
  label,
  value,
  series,
}: {
  label: string;
  value: string;
  series: (number | null)[];
}) {
  return (
    <Paper withBorder p="md" radius="sm">
      <Stack gap={4}>
        <Text size="sm" c="dimmed">
          {label}
        </Text>
        <Text fw={700} size="xl">
          {value}
        </Text>
        {hasTrend(series) ? (
          <Sparkline
            h="2.25rem"
            w="100%"
            data={series}
            curveType="linear"
            color="brand.6"
            fillOpacity={0.15}
            connectNulls={false}
            aria-hidden
          />
        ) : null}
      </Stack>
    </Paper>
  );
}

function hasTrend(series: (number | null)[]): boolean {
  let count = 0;
  for (const value of series) {
    if (value != null) {
      count += 1;
    }
    if (count >= 2) {
      return true;
    }
  }
  return false;
}
