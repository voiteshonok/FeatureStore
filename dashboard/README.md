# FeatureStore dashboard

Operator SPA (Vite + React 18 + TypeScript + Mantine + Redux Toolkit).

## Scripts

```bash
npm ci
npm run gen:api      # types from docs/openapi/gateway.json
npm run lint
npm run format:check
npm run typecheck
npm run test
npm run build
npm run dev          # http://localhost:5173, proxies /api → :8080
```

## Docker

Built from the repo root via `dashboard/Dockerfile` and served by nginx on port 8081 (`make up`).
`/api/` is proxied to the gateway; SSE under `/api/v1/stream/` disables buffering.
