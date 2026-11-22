import { Group, Stack, Text, Title } from "@mantine/core";

import { AlertActionStub } from "@features/session";

export default function OverviewPage() {
  return (
    <Stack gap="md">
      <Group justify="space-between" align="flex-start">
        <div>
          <Title order={2}>Overview</Title>
          <Text c="dimmed" mt="xs">
            Live stream panels arrive in T8.3.
          </Text>
        </div>
        <AlertActionStub />
      </Group>
    </Stack>
  );
}
