import { Grid, Skeleton, Stack } from "@mantine/core";

export function OverviewSkeleton() {
  return (
    <Stack gap="md" role="status" aria-busy="true" aria-label="Loading overview">
      <Grid>
        {["a", "b", "c", "d"].map((key) => (
          <Grid.Col key={key} span={{ base: 6, lg: 3 }}>
            <Skeleton height="4.5rem" radius="sm" />
          </Grid.Col>
        ))}
      </Grid>
      <Skeleton height="12rem" radius="sm" />
      <Skeleton height="10rem" radius="sm" />
    </Stack>
  );
}
