import { AppShell, Burger, Group, NavLink, Text, useMantineColorScheme } from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { Link, Outlet, useLocation } from "react-router-dom";

import { ColorSchemeToggle } from "@app/layout/ColorSchemeToggle";
import { ErrorBoundary } from "@app/ErrorBoundary";

const NAV = [
  { to: "/", label: "Overview" },
  { to: "/alerts", label: "Alerts" },
  { to: "/features", label: "Features" },
  { to: "/login", label: "Sign in" },
] as const;

export function AppLayout() {
  const [opened, { toggle }] = useDisclosure();
  const location = useLocation();
  const { colorScheme } = useMantineColorScheme();

  return (
    <AppShell
      header={{ height: 56 }}
      navbar={{ width: 220, breakpoint: "sm", collapsed: { mobile: !opened } }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group>
            <Burger opened={opened} onClick={toggle} hiddenFrom="sm" size="sm" aria-label="Menu" />
            <Text
              fw={700}
              component={Link}
              to="/"
              style={{ textDecoration: "none", color: "inherit" }}
            >
              FeatureStore
            </Text>
          </Group>
          <Group gap="sm">
            <Text size="sm" c="dimmed">
              {colorScheme}
            </Text>
            <ColorSchemeToggle />
          </Group>
        </Group>
      </AppShell.Header>

      <AppShell.Navbar p="md">
        {NAV.map((item) => (
          <NavLink
            key={item.to}
            component={Link}
            to={item.to}
            label={item.label}
            active={
              item.to === "/"
                ? location.pathname === "/"
                : location.pathname === item.to || location.pathname.startsWith(`${item.to}/`)
            }
          />
        ))}
      </AppShell.Navbar>

      <AppShell.Main>
        <ErrorBoundary title="This page crashed">
          <Outlet />
        </ErrorBoundary>
      </AppShell.Main>
    </AppShell>
  );
}
