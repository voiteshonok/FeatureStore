import { Paper, Stack, Text, Title } from "@mantine/core";
import { LineChart } from "@mantine/charts";
import { useSelector } from "react-redux";

import { formatRate } from "../model/format";
import { selectEventsSeries, selectLatestStats } from "../model/streamSelectors";
import type { StreamState } from "../model/streamSlice";

export function EventsChart() {
  const series = useSelector((state: { stream: StreamState }) => selectEventsSeries(state));
  const latest = useSelector((state: { stream: StreamState }) => selectLatestStats(state));
  const summary = latest
    ? `Current events/s: ${formatRate(latest.events_per_s)}`
    : "Waiting for samples";

  return (
    <Paper withBorder p="md" radius="sm">
      <Stack gap="sm">
        <Title order={3}>Events over 5 minutes</Title>
        <Text size="sm" c="dimmed">
          {summary}
        </Text>
        {series.length === 0 ? null : (
          <LineChart
            h="16rem"
            data={series}
            dataKey="time"
            series={[{ name: "eventsPerS", label: "Events/s", color: "brand.6" }]}
            curveType="linear"
            withDots={false}
            strokeWidth={2}
            gridAxis="xy"
            xAxisProps={{ minTickGap: 48 }}
            valueFormatter={(value) => formatRate(value)}
          />
        )}
      </Stack>
    </Paper>
  );
}
