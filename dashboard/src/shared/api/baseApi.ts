import { createApi } from "@reduxjs/toolkit/query/react";

import { baseQueryWithErrors } from "@shared/api/baseQuery";
import type { components, paths } from "@shared/api/schema";
import type { ApiFailure } from "@shared/api/problem";
import { notify } from "@shared/lib/notify";

type CatalogResponse =
  paths["/v1/catalog"]["get"]["responses"]["200"]["content"]["application/json"];
type AlertUpdate = components["schemas"]["AlertUpdate"];
type AlertDetail =
  paths["/v1/alerts/{alert_id}"]["get"]["responses"]["200"]["content"]["application/json"];
type AlertOut = components["schemas"]["AlertOut"];
type AlertList = components["schemas"]["AlertListOut"];
type RuleOut = components["schemas"]["RuleOut"];
type FeatureRequest = components["schemas"]["FeatureRequest"];
type FeatureResponse = components["schemas"]["FeatureResponse"];

export type AlertsListArgs = NonNullable<paths["/v1/alerts"]["get"]["parameters"]["query"]>;

/**
 * Shared RTK Query API. Cookie session + CSRF header (ADR-020).
 * Auth endpoints are injected from features/session.
 */
export const baseApi = createApi({
  reducerPath: "api",
  baseQuery: baseQueryWithErrors,
  tagTypes: ["Alert", "Rule", "Catalog", "Session"],
  endpoints: (build) => ({
    getCatalog: build.query<CatalogResponse, undefined>({
      query: () => "/catalog",
      providesTags: ["Catalog"],
    }),
    lookupFeatures: build.mutation<FeatureResponse, FeatureRequest>({
      query: (body) => ({
        url: "/features",
        method: "POST",
        body,
      }),
    }),
    getAlerts: build.query<AlertList, AlertsListArgs>({
      query: (args) => ({
        url: "/alerts",
        params: args,
      }),
      providesTags: (result) => {
        const items = result?.items ?? [];
        return [
          { type: "Alert" as const, id: "LIST" },
          ...items.map((item) => ({ type: "Alert" as const, id: item.alert_id })),
        ];
      },
    }),
    getRules: build.query<RuleOut[], undefined>({
      query: () => "/rules",
      providesTags: ["Rule"],
    }),
    getAlert: build.query<AlertDetail, string>({
      query: (alertId) => `/alerts/${alertId}`,
      providesTags: (_result, _error, alertId) => [{ type: "Alert", id: alertId }],
    }),
    patchAlert: build.mutation<AlertOut, { alertId: string; body: AlertUpdate }>({
      query: ({ alertId, body }) => ({
        url: `/alerts/${alertId}`,
        method: "PATCH",
        body,
      }),
      extraOptions: { silentStatuses: [409] },
      invalidatesTags: (_result, _error, arg) => [{ type: "Alert", id: arg.alertId }],
      async onQueryStarted({ alertId, body }, api) {
        const patches = [
          api.dispatch(
            baseApi.util.updateQueryData("getAlert", alertId, (draft) => {
              draft.alert.status = body.status;
            }),
          ),
        ];
        const cached = baseApi.util.selectCachedArgsForQuery(api.getState(), "getAlerts");
        for (const args of cached) {
          patches.push(
            api.dispatch(
              baseApi.util.updateQueryData("getAlerts", args, (draft) => {
                const item = draft.items.find((row) => row.alert_id === alertId);
                if (item) {
                  item.status = body.status;
                }
              }),
            ),
          );
        }
        try {
          await api.queryFulfilled;
        } catch (caught) {
          for (const patch of patches) {
            patch.undo();
          }
          if (!isConflict(caught)) {
            return;
          }
          try {
            const detail = await api
              .dispatch(baseApi.endpoints.getAlert.initiate(alertId, { forceRefetch: true }))
              .unwrap();
            const actor = latestActor(detail.history);
            notify(
              actor ? `Status was already changed by ${actor}` : "Status was already changed",
              "warning",
            );
          } catch {
            notify("Status was already changed", "warning");
          }
        }
      },
    }),
  }),
});

export const {
  useGetCatalogQuery,
  useLookupFeaturesMutation,
  useGetAlertsQuery,
  useGetRulesQuery,
  useGetAlertQuery,
  usePatchAlertMutation,
} = baseApi;

function isConflict(value: unknown): value is { error: ApiFailure } {
  return (
    typeof value === "object" &&
    value !== null &&
    "error" in value &&
    typeof value.error === "object" &&
    value.error !== null &&
    "status" in value.error &&
    value.error.status === 409
  );
}

function latestActor(history: { audit_id: number; actor: string | null }[]): string | null {
  let best: { audit_id: number; actor: string | null } | null = null;
  for (const entry of history) {
    if (!best || entry.audit_id > best.audit_id) {
      best = entry;
    }
  }
  return best?.actor && best.actor.length > 0 ? best.actor : null;
}
