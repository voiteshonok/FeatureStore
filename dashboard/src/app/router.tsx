import { lazy, Suspense, type ReactNode } from "react";
import { createBrowserRouter, Navigate } from "react-router-dom";

import { AppLayout } from "@app/layout/AppLayout";
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

export const router = createBrowserRouter([
  {
    path: "/",
    element: <AppLayout />,
    children: [
      { index: true, element: withSuspense(<OverviewPage />) },
      { path: "alerts", element: withSuspense(<AlertsPage />) },
      { path: "alerts/:alertId", element: withSuspense(<AlertDetailPage />) },
      { path: "features", element: withSuspense(<FeaturesPage />) },
      { path: "login", element: withSuspense(<LoginPage />) },
      { path: "forbidden", element: withSuspense(<ForbiddenPage />) },
      { path: "404", element: withSuspense(<NotFoundPage />) },
      { path: "*", element: <Navigate to="/404" replace /> },
    ],
  },
]);
