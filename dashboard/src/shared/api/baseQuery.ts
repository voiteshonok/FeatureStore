import {
  type BaseQueryFn,
  type FetchArgs,
  type FetchBaseQueryError,
  fetchBaseQuery,
} from "@reduxjs/toolkit/query";

import { isGetRequest, isLoginRequest, parseProblem, type ApiFailure } from "@shared/api/problem";
import { emitSessionExpired } from "@shared/lib/authEvents";
import { messageForErrorType, serviceUnavailableMessage } from "@shared/lib/errorMessages";
import { logger } from "@shared/lib/logger";
import { notify } from "@shared/lib/notify";
import { safeNextPath } from "@shared/lib/safeNext";

export type BaseQueryExtra = {
  /** Redirect to /forbidden on 403 (page-load queries). */
  forbiddenRedirect?: boolean;
  /** Skip global toasts / redirects (rare). */
  silent?: boolean;
  /** HTTP statuses that should not raise a global toast. The caller handles them. */
  silentStatuses?: number[];
};

function createRawBaseQuery() {
  const origin =
    typeof window !== "undefined" && window.location.origin
      ? window.location.origin
      : "http://localhost";
  return fetchBaseQuery({
    baseUrl: `${origin}/api/v1`,
    credentials: "include",
    prepareHeaders(headers) {
      headers.set("X-Requested-With", "fetch");
      return headers;
    },
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function retryAfterSeconds(meta: unknown): number | undefined {
  if (!meta || typeof meta !== "object" || !("response" in meta)) {
    return undefined;
  }
  const response = (meta as { response?: Response }).response;
  const header = response?.headers.get("Retry-After");
  if (!header) {
    return undefined;
  }
  const seconds = Number.parseInt(header, 10);
  return Number.isFinite(seconds) ? seconds : undefined;
}

function toFailure(error: FetchBaseQueryError, meta?: unknown): ApiFailure {
  if (
    error.status === "FETCH_ERROR" ||
    error.status === "PARSING_ERROR" ||
    error.status === "TIMEOUT_ERROR"
  ) {
    return { status: error.status, raw: error };
  }
  if (typeof error.status === "number") {
    const problem = parseProblem(error.data);
    return {
      status: error.status,
      type: problem?.type,
      title: problem?.title,
      requestId: problem?.request_id,
      fieldErrors: problem?.errors,
      retryAfterSec: retryAfterSeconds(meta),
      raw: error.data,
    };
  }
  return { status: "CUSTOM_ERROR", raw: error };
}

function redirectToLogin(): void {
  const current = `${window.location.pathname}${window.location.search}`;
  if (current.startsWith("/login")) {
    return;
  }
  const next = encodeURIComponent(safeNextPath(current));
  window.location.assign(`/login?next=${next}`);
}

export const baseQueryWithErrors: BaseQueryFn<
  string | FetchArgs,
  unknown,
  ApiFailure,
  BaseQueryExtra
> = async (args, api, extra) => {
  const options: BaseQueryExtra = { ...extra };
  const silent = options.silent === true;
  const maxAttempts = isGetRequest(args) ? 3 : 1;
  let lastError: FetchBaseQueryError | undefined;
  let lastMeta: unknown;
  const rawBaseQuery = createRawBaseQuery();

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const result = await rawBaseQuery(args, api, options);
    if (result.data !== undefined) {
      return { data: result.data };
    }
    lastError = result.error;
    lastMeta = result.meta;
    const status = lastError?.status;
    const retryable =
      typeof status === "number"
        ? status >= 500
        : status === "FETCH_ERROR" || status === "TIMEOUT_ERROR";
    if (!retryable || attempt === maxAttempts) {
      break;
    }
    await sleep(250 * attempt);
  }

  if (!lastError) {
    return { error: { status: "CUSTOM_ERROR" } };
  }

  const failure = toFailure(lastError, lastMeta);
  const loginCall = isLoginRequest(args);

  if (failure.status === 401) {
    emitSessionExpired();
    if (!loginCall && !silent) {
      redirectToLogin();
    }
    return { error: failure };
  }

  if (silent) {
    return { error: failure };
  }

  if (failure.status === 403) {
    notify(messageForErrorType(failure.type ?? "forbidden"));
    if (options.forbiddenRedirect) {
      window.location.assign("/forbidden");
    }
    return { error: failure };
  }

  if (failure.status === 409) {
    if (!options.silentStatuses?.includes(409)) {
      notify(messageForErrorType(failure.type ?? "conflict"), "warning");
    }
    return { error: failure };
  }

  if (failure.status === 422) {
    return { error: failure };
  }

  if (failure.status === 429) {
    const wait = failure.retryAfterSec;
    const base = messageForErrorType("rate_limited");
    notify(wait && Number.isFinite(wait) ? `${base}. Try again in ${wait}s` : base, "warning");
    return { error: failure };
  }

  if (
    failure.status === "FETCH_ERROR" ||
    failure.status === "TIMEOUT_ERROR" ||
    (typeof failure.status === "number" && failure.status >= 500)
  ) {
    notify(serviceUnavailableMessage(failure.requestId));
    logger.warn("upstream failure", { status: failure.status, type: failure.type });
    return { error: failure };
  }

  if (typeof failure.status === "number" && failure.status >= 400) {
    notify(messageForErrorType(failure.type));
  }

  return { error: failure };
};
