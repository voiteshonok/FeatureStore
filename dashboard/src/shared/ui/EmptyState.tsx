import { Button, Stack, Text, Title } from "@mantine/core";
import { Link } from "react-router-dom";

type EmptyStateProps = {
  title: string;
  description?: string;
  actionLabel?: string;
  actionTo?: string;
  onAction?: () => void;
};

export function EmptyState({
  title,
  description,
  actionLabel,
  actionTo,
  onAction,
}: EmptyStateProps) {
  return (
    <Stack gap="sm" py="xl" align="flex-start">
      <Title order={3}>{title}</Title>
      {description ? <Text c="dimmed">{description}</Text> : null}
      {actionLabel && actionTo ? (
        <Button component={Link} to={actionTo} variant="light">
          {actionLabel}
        </Button>
      ) : null}
      {actionLabel && onAction ? (
        <Button variant="light" onClick={onAction}>
          {actionLabel}
        </Button>
      ) : null}
    </Stack>
  );
}
