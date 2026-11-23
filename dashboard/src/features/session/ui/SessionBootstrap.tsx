import { type ReactNode } from "react";
import { useSelector } from "react-redux";

import { PageSkeleton } from "@shared/ui/PageSkeleton";

import { useGetMeQuery } from "../api/authApi";
import { selectSessionStatus, type SessionState } from "../model/sessionSlice";

type Props = { children: ReactNode };

/** Resolve session via GET /auth/me before showing routes (no login flash). */
export function SessionBootstrap({ children }: Props) {
  const status = useSelector((state: { session: SessionState }) => selectSessionStatus(state));
  useGetMeQuery(undefined);

  if (status === "unknown") {
    return <PageSkeleton />;
  }
  return children;
}
