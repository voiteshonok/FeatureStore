import {
  AppShell,
  Button,
  Group,
  Menu,
  NavLink,
  Text,
  useMantineColorScheme,
  useMantineTheme,
} from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import { IconBell, IconLayoutDashboard, IconSearch } from "@tabler/icons-react";
import { useSelector } from "react-redux";
import { Link, Outlet, useLocation, useNavigate } from "react-router-dom";

import { ErrorBoundary } from "@app/ErrorBoundary";
import { ColorSchemeToggle } from "@app/layout/ColorSchemeToggle";
import { StaleBanner } from "@features/stream/ui/StaleBanner";
import { selectIsAuthenticated, useLogoutMutation, type SessionState } from "@features/session";

import classes from "./AppLayout.module.css";

const NAV = [
  { to: "/", label: "Overview", icon: IconLayoutDashboard },
  { to: "/alerts", label: "Alerts", icon: IconBell },
  { to: "/features", label: "Features", icon: IconSearch },
] as const;

export function AppLayout() {
  const location = useLocation();
  const navigate = useNavigate();
  const theme = useMantineTheme();
  const wide = useMediaQuery(`(min-width: ${theme.breakpoints.lg})`, false, {
    getInitialValueInEffect: false,
  });
  const { colorScheme } = useMantineColorScheme();
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
      header={{ height: "3.5rem" }}
      navbar={{
        width: { sm: "4.5rem", lg: "16rem" },
        breakpoint: "sm",
        collapsed: { mobile: true, desktop: !authenticated },
      }}
      footer={{
        height: { base: "4.5rem", sm: 0 },
        collapsed: !authenticated,
      }}
      padding="md"
    >
      <AppShell.Header>
        <Group h="100%" px="md" justify="space-between" wrap="nowrap">
          <Text fw={700} component={Link} to={authenticated ? "/" : "/login"} td="none" c="inherit">
            FeatureStore
          </Text>
          <Group gap="sm" wrap="nowrap">
            <Text size="sm" c="dimmed" visibleFrom="sm">
              {colorScheme}
            </Text>
            <ColorSchemeToggle />
            {authenticated ? (
              <Menu shadow="md" width="16rem">
                <Menu.Target>
                  <Button variant="default" size="compact-sm" className={classes.account}>
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
        <AppShell.Navbar p="xs">
          {NAV.map((item) => {
            const Icon = item.icon;
            const active = isActive(item.to, location.pathname);
            return (
              <NavLink
                key={item.to}
                className={wide ? undefined : classes.iconOnly}
                component={Link}
                to={item.to}
                label={wide ? item.label : undefined}
                aria-label={wide ? undefined : item.label}
                leftSection={<Icon size={20} />}
                active={active}
              />
            );
          })}
        </AppShell.Navbar>
      ) : null}

      {authenticated ? (
        <AppShell.Footer>
          <nav className={classes.tabs} aria-label="Primary">
            {NAV.map((item) => {
              const Icon = item.icon;
              const active = isActive(item.to, location.pathname);
              return (
                <Link
                  key={item.to}
                  to={item.to}
                  className={active ? `${classes.tab} ${classes.tabActive}` : classes.tab}
                  aria-current={active ? "page" : undefined}
                >
                  <Icon size={20} aria-hidden />
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </AppShell.Footer>
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

function isActive(to: string, pathname: string): boolean {
  if (to === "/") {
    return pathname === "/";
  }
  return pathname === to || pathname.startsWith(`${to}/`);
}
