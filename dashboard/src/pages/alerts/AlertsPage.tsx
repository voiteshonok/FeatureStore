import { Group, Stack, Text, Title } from "@mantine/core";

import { AlertActionStub } from "@features/session";
import { EmptyState } from "@shared/ui/EmptyState";

export default function AlertsPage() {
  return (
    <Stack gap="md">
      <Group justify="space-between" align="flex-start">
        <div>
          <Title order={2}>Alerts</Title>
          <Text c="dimmed" mt="xs">
            Filtered list lands in T8.4.
          </Text>
        </div>
        <AlertActionStub />
      </Group>
      <EmptyState title="No alerts" description="When rule events fire, they will show up here." />
    </Stack>
  );
}
