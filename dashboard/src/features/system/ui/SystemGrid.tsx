import { Alert, Badge, Group, Paper, SimpleGrid, Stack, Text, Title } from "@mantine/core";
import { useSelector } from "react-redux";

import { selectStreamSystem } from "@features/stream/model/streamSelectors";
import type { StreamState } from "@features/stream/model/streamSlice";

import {
  SYSTEM_COMPONENT_ORDER,
  componentStatusColor,
  componentStatusCopy,
  systemModeCopy,
} from "../model/statusCopy";

export function SystemGrid() {
  const snapshot = useSelector((state: { stream: StreamState }) => selectStreamSystem(state));
  const byName = new Map(snapshot?.components.map((item) => [item.name, item]));
  const modeCopy = snapshot ? systemModeCopy(snapshot.mode) : null;

  return (
    <Paper withBorder p="md" radius="sm">
      <Stack gap="sm">
        <Title order={3}>System</Title>
        {modeCopy ? (
          <Alert
            variant="light"
            color={snapshot?.mode === "fallback" ? "orange" : "red"}
            role="status"
          >
            {modeCopy}
          </Alert>
        ) : null}
        {snapshot == null ? (
          <Text c="dimmed" size="sm">
            Waiting for system status
          </Text>
        ) : (
          <SimpleGrid cols={{ base: 1, xs: 2 }}>
            {SYSTEM_COMPONENT_ORDER.map((name) => {
              const component = byName.get(name);
              const status = component?.status ?? "unknown";
              const label = componentStatusCopy(status);
              return (
                <Group key={name} justify="space-between" wrap="nowrap" gap="xs">
                  <Stack gap={0}>
                    <Text fw={600} size="sm">
                      {name}
                    </Text>
                    {component?.lag != null ? (
                      <Text size="xs" c="dimmed">
                        lag {component.lag}
                      </Text>
                    ) : null}
                  </Stack>
                  <Badge color={componentStatusColor(status)} variant="light">
                    {label}
                  </Badge>
                </Group>
              );
            })}
          </SimpleGrid>
        )}
      </Stack>
    </Paper>
  );
}
