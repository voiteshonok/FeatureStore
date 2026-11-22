import { baseApi } from "@shared/api/baseApi";
import type { components } from "@shared/api/schema";

import { sessionAnonymous, sessionResolved } from "../model/sessionSlice";

type MeOut = components["schemas"]["MeOut"];
type LoginBody = components["schemas"]["LoginBody"];

function applyMe(dispatch: (action: unknown) => void, me: MeOut): void {
  dispatch(
    sessionResolved({
      operatorId: me.operator_id,
      email: me.email,
      role: me.role,
    }),
  );
}

export const authApi = baseApi.injectEndpoints({
  endpoints: (build) => ({
    getMe: build.query<MeOut, undefined>({
      query: () => "/auth/me",
      providesTags: ["Session"],
      async onQueryStarted(_arg, { dispatch, queryFulfilled }) {
        try {
          const { data } = await queryFulfilled;
          applyMe(dispatch, data);
        } catch {
          dispatch(sessionAnonymous());
        }
      },
    }),
    login: build.mutation<undefined, LoginBody>({
      query: (body) => ({
        url: "/auth/login",
        method: "POST",
        body,
      }),
      async onQueryStarted(_arg, { dispatch, queryFulfilled }) {
        try {
          await queryFulfilled;
          const me = await dispatch(
            authApi.endpoints.getMe.initiate(undefined, { forceRefetch: true }),
          ).unwrap();
          applyMe(dispatch, me);
        } catch {
          // Form surfaces unauthorized / rate_limited.
        }
      },
    }),
    logout: build.mutation<undefined, undefined>({
      query: () => ({
        url: "/auth/logout",
        method: "POST",
      }),
      async onQueryStarted(_arg, { dispatch, queryFulfilled }) {
        try {
          await queryFulfilled;
        } finally {
          dispatch(sessionAnonymous());
          dispatch(baseApi.util.resetApiState());
        }
      },
    }),
  }),
});

export const { useGetMeQuery, useLoginMutation, useLogoutMutation } = authApi;
