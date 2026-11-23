import { Button, useComputedColorScheme, useMantineColorScheme } from "@mantine/core";
import { IconMoon, IconSun } from "@tabler/icons-react";

export function ColorSchemeToggle() {
  const { setColorScheme } = useMantineColorScheme();
  const computed = useComputedColorScheme("light", { getInitialValueInEffect: false });
  const next = computed === "dark" ? "light" : "dark";
  const Icon = computed === "dark" ? IconSun : IconMoon;

  return (
    <Button
      variant="default"
      size="compact-sm"
      px="xs"
      aria-label={`Switch to ${next} theme`}
      onClick={() => {
        setColorScheme(next);
      }}
    >
      <Icon size={18} aria-hidden />
    </Button>
  );
}
