import { Button, useMantineColorScheme } from "@mantine/core";

export function ColorSchemeToggle() {
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  const next = colorScheme === "dark" ? "light" : "dark";

  return (
    <Button
      variant="default"
      size="compact-sm"
      aria-label={`Switch to ${next} theme`}
      onClick={() => {
        setColorScheme(next);
      }}
    >
      {next}
    </Button>
  );
}
