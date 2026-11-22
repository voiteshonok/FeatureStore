import { Paper, Stack, Text, Title, useMantineTheme } from "@mantine/core";
import { BarChart } from "@mantine/charts";
import { useMediaQuery } from "@mantine/hooks";
import { useSelector } from "react-redux";

import { selectAlertsByRule } from "../model/streamSelectors";
import type { StreamState } from "../model/streamSlice";

export function AlertsByRuleChart() {
  const theme = useMantineTheme();
  const phone = useMediaQuery(`(max-width: ${theme.breakpoints.sm})`, false, {
    getInitialValueInEffect: false,
  });
  const rows = useSelector((state: { stream: StreamState }) => selectAlertsByRule(state));

  return (
    <Paper withBorder p="md" radius="sm">
      <Stack gap="sm">
        <Title order={3}>Alerts in the last minute</Title>
        {rows.length === 0 ? (
          <Text c="dimmed" size="sm">
            No alerts in the last minute
          </Text>
        ) : (
          <BarChart
            h="16rem"
            data={rows}
            dataKey="rule"
            orientation="vertical"
            series={[{ name: "count", label: "Alerts", color: "brand.6" }]}
            withBarValueLabel
            gridAxis="none"
            yAxisProps={{ width: phone ? 72 : 112 }}
          />
        )}
      </Stack>
    </Paper>
  );
}
