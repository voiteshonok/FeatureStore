import { Skeleton, Stack } from "@mantine/core";

/** Generic page placeholder used under Suspense for lazy routes. */
export function PageSkeleton() {
  return (
    <Stack gap="md" role="status" p="md" aria-busy="true" aria-label="Loading page">
      <Skeleton height={28} width="40%" radius="sm" />
      <Skeleton height={120} radius="sm" />
      <Skeleton height={120} radius="sm" />
    </Stack>
  );
}
