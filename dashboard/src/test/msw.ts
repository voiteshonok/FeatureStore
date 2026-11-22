import { http, HttpResponse } from "msw";
import { setupServer } from "msw/node";

export const handlers = [
  http.get("/api/v1/auth/me", () =>
    HttpResponse.json(
      { type: "unauthorized", title: "missing", status: 401, request_id: "t" },
      { status: 401 },
    ),
  ),
];

export const server = setupServer(...handlers);
