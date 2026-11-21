# ADR-020: Operator authentication

- **Status:** Accepted
- **Date:** 2026-11-21
- **Related:** US-6.5, ADR-008, ADR-009, ADR-019; supersedes ADR-008 on JWT/OIDC scope

## Context

The dashboard must run as a named operator with a role (`viewer` / `analyst` /
`admin`) without embedding an API key in the frontend bundle. Scoring clients stay
on API keys. Operator identity for alert mutations already arrives as gateway
headers (ADR-019); this ADR defines how the gateway authenticates humans and sets
those headers.

## Decision

- **Edge auth (ADR-008).** Login, session verification, CSRF checks, and RBAC live
  in the gateway. Upstream services trust `X-Operator-Id` / `X-Operator-Role` only
  from the gateway; client-supplied copies are stripped.
- **Dual credentials.** Machine clients present `X-API-Key`. Operators present
  cookie `fs_session`. The former `dashboard` API key is removed.
- **Password check.** `svc_gateway` has a column grant
  `SELECT (operator_id, email, password_hash, role) ON ops.operators` and nothing
  else on that table. Verification uses `bcrypt.checkpw` in Python (pgcrypto `$2a$`
  hashes). The password never appears in SQL.
- **Token.** JWT HS256 via PyJWT. Claims: `sub` = operator_id, `role`, `iat`,
  `exp` (1 hour), `jti`. Signing key is compose secret `jwt_secret` (same
  `make secret` pattern as ADR-009).
- **Cookie.** `fs_session`: HttpOnly, SameSite=Strict, Path=`/api`, Secure in
  production. Sliding renewal: if less than 15 minutes remain, the gateway
  re-issues the cookie. No refresh tokens and no server-side denylist.
- **CSRF.** Cookie-authenticated `POST` / `PATCH` / `DELETE` require
  `X-Requested-With: fetch`.
- **RBAC at the gateway.**

  | Route | scoring (key) | viewer | analyst | admin |
  | --- | --- | --- | --- | --- |
  | `POST /v1/features` | yes | yes | yes | yes |
  | GET alerts / rules / catalog / streams | no | yes | yes | yes |
  | `PATCH /v1/alerts/{id}` | no | 403 | yes | yes |

## Alternatives considered

- **API key in the dashboard bundle** — forbidden by the Frontend criterion
  (tokens in code) and shared across operators.
- **OIDC / external IdP** — correct for production multi-tenant auth; out of diploma
  scope.
- **Auth in detection** — splits the edge concern and duplicates RBAC; rejected by
  ADR-008.

## Consequences

- Routes: `POST /v1/auth/login`, `POST /v1/auth/logout`, `GET /v1/auth/me`.
- Login is rate-limited to 5 attempts per minute per IP; failed logins use a
  constant-time dummy `checkpw` so missing emails are not distinguishable by
  latency.
- Out of scope: token revocation before expiry, registration, password reset,
  OAuth/OIDC.
- Detection keeps header-based actor checks (ADR-019) as a second line of defense.
