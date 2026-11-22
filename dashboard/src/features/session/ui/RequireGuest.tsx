import { type ReactNode } from "react";
import { useSelector } from "react-redux";
import { Navigate, useSearchParams } from "react-router-dom";

import { safeNextPath } from "@shared/lib/safeNext";
import { PageSkeleton } from "@shared/ui/PageSkeleton";

import {
  selectIsAuthenticated,
  selectSessionStatus,
  type SessionState,
} from "../model/sessionSlice";

type Props = { children: ReactNode };

export function RequireGuest({ children }: Props) {
  const status = useSelector((state: { session: SessionState }) => selectSessionStatus(state));
  const ok = useSelector((state: { session: SessionState }) => selectIsAuthenticated(state));
  const [params] = useSearchParams();

  if (status === "unknown") {
    return <PageSkeleton />;
  }
  if (ok) {
    return <Navigate to={safeNextPath(params.get("next"))} replace />;
  }
  return children;
}
