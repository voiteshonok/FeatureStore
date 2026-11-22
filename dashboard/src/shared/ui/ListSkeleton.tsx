import { Skeleton, Stack } from "@mantine/core";

export function ListSkeleton({ rows = 6 }: { rows?: number }) {
  return (
    <Stack gap="sm" aria-busy="true" aria-label="Loading list">
      <Skeleton height={24} width="30%" radius="sm" />
      {Array.from({ length: rows }, (_, index) => (
        <Skeleton key={index} height={44} radius="sm" />
      ))}
    </Stack>
  );
}
