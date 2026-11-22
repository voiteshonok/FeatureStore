import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";

import type { paths } from "@shared/api/schema";

type CatalogResponse =
  paths["/v1/catalog"]["get"]["responses"]["200"]["content"]["application/json"];

/**
 * Shared RTK Query API. Cookie session + CSRF header (ADR-020).
 * Error mapping lands in T8.2.
 */
export const baseApi = createApi({
  reducerPath: "api",
  baseQuery: fetchBaseQuery({
    baseUrl: "/api/v1",
    credentials: "include",
    prepareHeaders(headers) {
      headers.set("X-Requested-With", "fetch");
      return headers;
    },
  }),
  tagTypes: ["Alert", "Rule", "Catalog", "Session"],
  endpoints: (build) => ({
    getCatalog: build.query<CatalogResponse, undefined>({
      query: () => "/catalog",
      providesTags: ["Catalog"],
    }),
  }),
});

export const { useGetCatalogQuery } = baseApi;
