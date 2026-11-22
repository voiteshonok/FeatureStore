import {
  AppShell,
  Burger,
  Button,
  Group,
  Menu,
  NavLink,
  Text,
  useMantineColorScheme,
} from "@mantine/core";
import { useDisclosure } from "@mantine/hooks";
import { useSelector } from "react-redux";
import { Link, Outlet, useLocation, useNavigate } from "react-router-dom";

import { ErrorBoundary } from "@app/ErrorBoundary";
import { ColorSchemeToggle } from "@app/layout/ColorSchemeToggle";
import { StaleBanner } from "@features/stream/ui/StaleBanner";
import {
  selectIsAuthenticated,
  selectSessionStatus,
  useLogoutMutation,
  type SessionState,
} from "@features/session";

const NAV = [
  { to: "/", label: "Overview" },
  { to: "/alerts", label: "Alerts" },
  { to: "/features", label: "Features" },
] as const;

export function AppLayout() {
  const [opened, { toggle }] = useDisclosure();
  const location = useLocation();
  const navigate = useNavigate();
  const { colorScheme } = useMantineColorScheme();
  const status = useSelector((state: { session: SessionState }) => selectSessionStatus(state));
  const authenticated = useSelector((state: { session: SessionState }) =>
    selectIsAuthenticated(state),
  );
  const session = useSelector((state: { session: SessionState }) => state.session);
  const [logout, { isLoading: loggingOut }] = useLogoutMutation();

  const onLogout = async () => {
    try {
      await logout(undefined).unwrap();
    } catch {
      // Cookie may already be gone.
    }
    navigate("/login", { replace: true });
  };

  return (
    <AppShell
      header={{ height: 56 }}
      navbar={{
        width: 220,
        breakpoint: "sm",
        collapsed: { mobile: !opened || status === "anonymous" },
      }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between">
          <Group>
            <Burger
              opened={opened}
              onClick={toggle}
              hiddenFrom="sm"
              size="sm"
              aria-label="Menu"
              disabled={!authenticated}
            />
            <Text
              fw={700}
              component={Link}
              to={authenticated ? "/" : "/login"}
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
            {authenticated ? (
              <Menu shadow="md" width={220}>
                <Menu.Target>
                  <Button variant="default" size="compact-sm">
                    {session.email}
                  </Button>
                </Menu.Target>
                <Menu.Dropdown>
                  <Menu.Label>{session.role}</Menu.Label>
                  <Menu.Item onClick={() => void onLogout()} disabled={loggingOut}>
                    Sign out
                  </Menu.Item>
                </Menu.Dropdown>
              </Menu>
            ) : null}
          </Group>
        </Group>
      </AppShell.Header>

      {authenticated ? (
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
      ) : null}

      <AppShell.Main>
        <StaleBanner />
        <ErrorBoundary title="This page crashed">
          <Outlet />
        </ErrorBoundary>
      </AppShell.Main>
    </AppShell>
  );
}
