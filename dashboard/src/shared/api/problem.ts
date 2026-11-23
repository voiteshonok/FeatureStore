import type { components } from "@shared/api/schema";

export type ProblemDetails = components["schemas"]["ProblemDetails"];
export type FieldErrorOut = components["schemas"]["FieldErrorOut"];

export type ApiFailure = {
  status: number | "FETCH_ERROR" | "PARSING_ERROR" | "TIMEOUT_ERROR" | "CUSTOM_ERROR";
  type?: string;
  title?: string;
  requestId?: string;
  fieldErrors?: FieldErrorOut[];
  retryAfterSec?: number;
  raw?: unknown;
};

export function parseProblem(data: unknown): ProblemDetails | null {
  if (!data || typeof data !== "object") {
    return null;
  }
  const body = data as Record<string, unknown>;
  if (typeof body.type !== "string" || typeof body.status !== "number") {
    return null;
  }
  return body as ProblemDetails;
}

export function isLoginRequest(args: string | { url?: string }): boolean {
  const url = typeof args === "string" ? args : (args.url ?? "");
  return url.includes("/auth/login");
}

export function isGetRequest(args: string | { method?: string }): boolean {
  if (typeof args === "string") {
    return true;
  }
  const method = (args.method ?? "GET").toUpperCase();
  return method === "GET";
}
