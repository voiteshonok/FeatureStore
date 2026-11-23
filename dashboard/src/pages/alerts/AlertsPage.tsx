import { useEffect, useState } from "react";
import { Stack, Title } from "@mantine/core";
import { useDispatch, useSelector } from "react-redux";
import { useSearchParams } from "react-router-dom";

import { selectPendingCreated, selectRuleNames } from "@features/stream/model/streamSelectors";
import { streamPendingCleared, type StreamState } from "@features/stream/model/streamSlice";
import { baseApi, useGetAlertsQuery, useGetRulesQuery } from "@shared/api/baseApi";
import { EmptyState } from "@shared/ui/EmptyState";
import { ListSkeleton } from "@shared/ui/ListSkeleton";

import { AlertFilters } from "@features/alerts/ui/AlertFilters";
import { AlertRows } from "@features/alerts/ui/AlertRows";
import { NewAlertsBanner } from "@features/alerts/ui/NewAlertsBanner";
import {
  EMPTY_ALERTS_FILTER,
  filtersActive,
  matchesCreatedAlert,
  matchesSearch,
  parseAlertsSearch,
  periodWindow,
  serverFilterKey,
  toListArgs,
  writeAlertsSearch,
  type AlertsFilter,
  type PeriodWindow,
} from "@features/alerts/model/filters";

type CacheState = {
  stream: StreamState;
  [baseApi.reducerPath]: ReturnType<typeof baseApi.reducer>;
};

export default function AlertsPage() {
  const dispatch = useDispatch();
  const [params, setParams] = useSearchParams();
  const filter = parseAlertsSearch(params);
  const serverKey = serverFilterKey(filter);
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const [seenKey, setSeenKey] = useState(serverKey);
  const [window, setWindow] = useState<PeriodWindow>(() => periodWindow(filter, Date.now()));
  const [draftQ, setDraftQ] = useState(filter.q);

  if (seenKey !== serverKey) {
    setSeenKey(serverKey);
    setCursors([undefined]);
    setWindow(periodWindow(filter, Date.now()));
  }

  useEffect(() => {
    setDraftQ(filter.q);
  }, [filter.q]);

  useEffect(() => {
    if (draftQ === filter.q) {
      return;
    }
    const timer = setTimeout(() => {
      setParams(writeAlertsSearch({ ...parseAlertsSearch(params), q: draftQ }), { replace: true });
    }, 300);
    return () => {
      clearTimeout(timer);
    };
  }, [draftQ, filter.q, params, setParams]);

  const latestCursor = cursors[cursors.length - 1];
  const latest = useGetAlertsQuery(toListArgs(filter, latestCursor, window));
  const rulesQuery = useGetRulesQuery(undefined);
  const ruleNames = useSelector((state: { stream: StreamState }) => selectRuleNames(state));
  const pending = useSelector((state: { stream: StreamState }) => selectPendingCreated(state));
  const pages = useSelector((state: CacheState) =>
    cursors.map(
      (cursor) =>
        baseApi.endpoints.getAlerts.select(toListArgs(filter, cursor, window))(state).data,
    ),
  );

  const loaded = pages.flatMap((page) => page?.items ?? []);
  const items = loaded.filter((item) => matchesSearch(item, filter.q));
  const nextCursor = pages[pages.length - 1]?.next_cursor ?? null;
  const firstLoading = latest.isLoading && cursors.length === 1;
  const fresh = pending.filter(
    (event) =>
      matchesCreatedAlert(event, filter, window) &&
      !loaded.some((item) => item.alert_id === event.alert_id),
  );

  const ruleName = (ruleId: string) =>
    ruleNames[ruleId] ?? rulesQuery.data?.find((rule) => rule.rule_id === ruleId)?.name ?? ruleId;

  const applyFilter = (next: AlertsFilter) => {
    setParams(writeAlertsSearch(next));
  };

  return (
    <Stack gap="md">
      <Title order={2}>Alerts</Title>
      <AlertFilters
        filter={filter}
        rules={rulesQuery.data ?? []}
        search={draftQ}
        onSearch={setDraftQ}
        onChange={applyFilter}
        onReset={() => {
          setDraftQ("");
          setParams(writeAlertsSearch(EMPTY_ALERTS_FILTER));
        }}
      />
      <NewAlertsBanner
        count={fresh.length}
        onShow={() => {
          dispatch(streamPendingCleared(fresh.map((event) => event.alert_id)));
          setCursors([undefined]);
          dispatch(baseApi.util.invalidateTags([{ type: "Alert", id: "LIST" }]));
        }}
      />
      {firstLoading ? <ListSkeleton /> : null}
      {!firstLoading && latest.isError && items.length === 0 ? (
        <EmptyState
          title="Could not load"
          description="The alert list did not load."
          actionLabel="Try again"
          onAction={() => {
            void latest.refetch();
          }}
        />
      ) : null}
      {!firstLoading && !latest.isError && items.length === 0 ? (
        filtersActive(filter) ? (
          <EmptyState
            title="Nothing matches"
            description="No alerts match these filters."
            actionLabel="Reset filters"
            onAction={() => {
              setDraftQ("");
              setParams(writeAlertsSearch(EMPTY_ALERTS_FILTER));
            }}
          />
        ) : (
          <EmptyState title="No alerts" description="When a rule fires, the alert shows up here." />
        )
      ) : null}
      {items.length > 0 ? (
        <AlertRows
          items={items}
          ruleName={ruleName}
          nextCursor={nextCursor}
          loadingMore={latest.isFetching && cursors.length > 1}
          onMore={() => {
            if (nextCursor && !cursors.includes(nextCursor)) {
              setCursors((current) => [...current, nextCursor]);
            }
          }}
        />
      ) : null}
    </Stack>
  );
}
