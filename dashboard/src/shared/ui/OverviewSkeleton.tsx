import { SimpleGrid, Skeleton, Stack } from "@mantine/core";

export function OverviewSkeleton() {
  return (
    <Stack gap="md" aria-busy="true" aria-label="Loading overview">
      <SimpleGrid cols={{ base: 1, sm: 2, md: 4 }}>
        <Skeleton height={72} radius="sm" />
        <Skeleton height={72} radius="sm" />
        <Skeleton height={72} radius="sm" />
        <Skeleton height={72} radius="sm" />
      </SimpleGrid>
      <Skeleton height={200} radius="sm" />
      <Skeleton height={160} radius="sm" />
    </Stack>
  );
}
