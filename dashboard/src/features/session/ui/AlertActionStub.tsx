import { Button } from "@mantine/core";
import { useSelector } from "react-redux";

import { selectCanChangeAlerts, type SessionState } from "../model/sessionSlice";

/** Placeholder action for T8.2 acceptance — hidden for viewer. */
export function AlertActionStub() {
  const canChange = useSelector((state: { session: SessionState }) => selectCanChangeAlerts(state));
  if (!canChange) {
    return null;
  }
  return (
    <Button variant="light" size="sm" disabled>
      Change status
    </Button>
  );
}
