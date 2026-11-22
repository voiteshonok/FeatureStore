import { createApi } from "@reduxjs/toolkit/query/react";

import { baseQueryWithErrors } from "@shared/api/baseQuery";
import type { components, paths } from "@shared/api/schema";

type CatalogResponse =
  paths["/v1/catalog"]["get"]["responses"]["200"]["content"]["application/json"];
type AlertUpdate = components["schemas"]["AlertUpdate"];
type AlertDetail =
  paths["/v1/alerts/{alert_id}"]["get"]["responses"]["200"]["content"]["application/json"];

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
    getAlert: build.query<AlertDetail, string>({
      query: (alertId) => `/alerts/${alertId}`,
      providesTags: (_result, _error, alertId) => [{ type: "Alert", id: alertId }],
    }),
    patchAlert: build.mutation<AlertDetail, { alertId: string; body: AlertUpdate }>({
      query: ({ alertId, body }) => ({
        url: `/alerts/${alertId}`,
        method: "PATCH",
        body,
      }),
      invalidatesTags: (_result, _error, arg) => [{ type: "Alert", id: arg.alertId }],
    }),
  }),
});

export const { useGetCatalogQuery, useGetAlertQuery, usePatchAlertMutation } = baseApi;
