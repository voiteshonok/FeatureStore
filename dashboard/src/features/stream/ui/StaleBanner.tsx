import { Text } from "@mantine/core";
import { useSelector } from "react-redux";

import { selectIsAuthenticated, type SessionState } from "@features/session";

import { formatClock } from "../model/format";
import { selectStaleSince } from "../model/streamSelectors";
import type { StreamState } from "../model/streamSlice";
import { useNow } from "./useNow";

import classes from "./StaleBanner.module.css";

export function StaleBanner() {
  const now = useNow(1_000);
  const authenticated = useSelector((state: { session: SessionState }) =>
    selectIsAuthenticated(state),
  );
  const since = useSelector((state: { stream: StreamState }) => selectStaleSince(state, now));
  if (!authenticated || since == null) {
    return null;
  }
  const clock = formatClock(since);
  const message = clock
    ? `Data is not updating since ${clock}. Reconnecting…`
    : "Data is not updating. Reconnecting…";
  return (
    <div className={classes.banner} role="status" aria-live="polite">
      <Text size="sm">{message}</Text>
    </div>
  );
}
