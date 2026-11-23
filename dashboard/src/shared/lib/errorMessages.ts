/** Client-facing copy keyed by ProblemDetails.type (docs/api-errors.md). */

const BY_TYPE: Record<string, string> = {
  unauthorized: "Please sign in",
  forbidden: "Insufficient permissions",
  not_found: "Not found",
  validation_error: "Check the form fields",
  invalid_transition: "Status was already changed",
  conflict: "Conflict — refresh and try again",
  rate_limited: "Too many requests",
  upstream_unavailable: "Service unavailable",
  upstream_timeout: "Service unavailable",
  internal_error: "Service unavailable",
  http_error: "Service unavailable",
  bad_request: "Bad request",
};

export function messageForErrorType(type: string | undefined): string {
  if (!type) {
    return "Service unavailable";
  }
  return BY_TYPE[type] ?? "Service unavailable";
}

export function serviceUnavailableMessage(requestId: string | undefined): string {
  if (requestId) {
    return `Service unavailable (request_id: ${requestId})`;
  }
  return "Service unavailable";
}
