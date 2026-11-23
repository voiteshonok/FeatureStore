import { Button, Group, Text } from "@mantine/core";

type NewAlertsBannerProps = {
  count: number;
  onShow: () => void;
};

export function NewAlertsBanner({ count, onShow }: NewAlertsBannerProps) {
  if (count <= 0) {
    return null;
  }
  const label = count === 1 ? "1 new alert" : `${count} new alerts`;
  return (
    <Group gap="sm">
      <Text size="sm">{label}</Text>
      <Button size="compact-sm" variant="light" onClick={onShow}>
        Show
      </Button>
    </Group>
  );
}
