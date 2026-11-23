import { Skeleton, Stack } from "@mantine/core";

export function CardSkeleton() {
  return (
    <Stack gap="md" role="status" aria-busy="true" aria-label="Loading card">
      <Skeleton height={28} width="50%" radius="sm" />
      <Skeleton height={16} width="70%" radius="sm" />
      <Skeleton height={120} radius="sm" />
      <Skeleton height={36} width={160} radius="sm" />
    </Stack>
  );
}
