import { Stack, Text, Title } from "@mantine/core";
import { useParams } from "react-router-dom";

import { AlertActionStub } from "@features/session";
import { useGetAlertQuery } from "@shared/api/baseApi";
import type { ApiFailure } from "@shared/api/problem";
import { CardSkeleton } from "@shared/ui/CardSkeleton";
import { EmptyState } from "@shared/ui/EmptyState";

function asFailure(error: unknown): ApiFailure | null {
  if (!error || typeof error !== "object") {
    return null;
  }
  if ("status" in error) {
    return error as ApiFailure;
  }
  return null;
}

export default function AlertDetailPage() {
  const { alertId = "" } = useParams();
  const { data, error, isLoading, isError } = useGetAlertQuery(alertId, { skip: !alertId });
  const failure = asFailure(error);

  if (!alertId) {
    return (
      <EmptyState
        title="Alert not found"
        description="Missing alert id."
        actionLabel="Back to alerts"
        actionTo="/alerts"
      />
    );
  }

  if (isLoading) {
    return <CardSkeleton />;
  }

  if (isError && failure?.status === 404) {
    return (
      <EmptyState
        title="Alert not found"
        description={`No alert ${alertId}.`}
        actionLabel="Back to alerts"
        actionTo="/alerts"
      />
    );
  }

  if (isError && failure?.status === 403) {
    return (
      <EmptyState
        title="Access denied"
        description="You do not have permission to view this alert."
        actionLabel="Back to overview"
        actionTo="/"
      />
    );
  }

  if (isError || !data) {
    return (
      <EmptyState
        title="Could not load"
        description="Try refreshing the page."
        actionLabel="Back to alerts"
        actionTo="/alerts"
      />
    );
  }

  const entity =
    data.alert.card_token != null
      ? `card ${data.alert.card_token}`
      : `merchant ${data.alert.merchant_id ?? "—"}`;

  return (
    <Stack gap="md">
      <div>
        <Title order={2}>{data.rule.name}</Title>
        <Text size="sm" c="dimmed" mt={4}>
          {data.alert.alert_id}
        </Text>
      </div>
      <Text size="sm" c="dimmed">
        Status: {data.alert.status} · {entity}
      </Text>
      <AlertActionStub />
    </Stack>
  );
}
