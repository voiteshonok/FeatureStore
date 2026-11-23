import { type ReactNode } from "react";
import { useSelector } from "react-redux";
import { Navigate, useLocation } from "react-router-dom";

import { PageSkeleton } from "@shared/ui/PageSkeleton";

import {
  selectIsAuthenticated,
  selectSessionStatus,
  type SessionState,
} from "../model/sessionSlice";

type Props = { children: ReactNode };

export function RequireAuth({ children }: Props) {
  const status = useSelector((state: { session: SessionState }) => selectSessionStatus(state));
  const ok = useSelector((state: { session: SessionState }) => selectIsAuthenticated(state));
  const location = useLocation();

  if (status === "unknown") {
    return <PageSkeleton />;
  }
  if (!ok) {
    const next = `${location.pathname}${location.search}`;
    return <Navigate to={`/login?next=${encodeURIComponent(next)}`} replace />;
  }
  return children;
}
