import { lazy, Suspense, type ReactNode } from "react";
import { createBrowserRouter, Navigate } from "react-router-dom";

import { AppLayout } from "@app/layout/AppLayout";
import { RequireAuth, RequireGuest } from "@features/session";
import { PageSkeleton } from "@shared/ui/PageSkeleton";

const LoginPage = lazy(() => import("@pages/login/LoginPage"));
const OverviewPage = lazy(() => import("@pages/overview/OverviewPage"));
const AlertsPage = lazy(() => import("@pages/alerts/AlertsPage"));
const AlertDetailPage = lazy(() => import("@pages/alert-detail/AlertDetailPage"));
const FeaturesPage = lazy(() => import("@pages/features/FeaturesPage"));
const ForbiddenPage = lazy(() => import("@pages/forbidden/ForbiddenPage"));
const NotFoundPage = lazy(() => import("@pages/not-found/NotFoundPage"));

function withSuspense(element: ReactNode) {
  return <Suspense fallback={<PageSkeleton />}>{element}</Suspense>;
}

function protectedPage(element: ReactNode) {
  return <RequireAuth>{withSuspense(element)}</RequireAuth>;
}

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppLayout />,
    children: [
      { index: true, element: protectedPage(<OverviewPage />) },
      { path: "alerts", element: protectedPage(<AlertsPage />) },
      { path: "alerts/:alertId", element: protectedPage(<AlertDetailPage />) },
      { path: "features", element: protectedPage(<FeaturesPage />) },
      {
        path: "login",
        element: <RequireGuest>{withSuspense(<LoginPage />)}</RequireGuest>,
      },
      { path: "forbidden", element: withSuspense(<ForbiddenPage />) },
      { path: "404", element: withSuspense(<NotFoundPage />) },
      { path: "*", element: <Navigate to="/404" replace /> },
    ],
  },
]);
