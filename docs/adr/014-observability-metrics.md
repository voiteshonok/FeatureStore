# ADR-014: Observability: metrics

- **Status:** Accepted
- **Date:** 2026-11-16
- **Related:** K1, K2, NFR-3; ADR-007, ADR-008

## Context

K1 is the feature-read latency and K2 is the time from ingest to online state.
Both are computed from Prometheus histograms, so every process has to expose
its own series. The online writer has no HTTP server (ADR-007). The gateway
port is published and closed by an API key (ADR-008), so a `/metrics` route on
that port would either sit behind authentication or be anonymous on a public
port.

## Decision

Each process uses `prometheus-client` and serves its registry with
`start_http_server` on `METRICS_PORT` (default 9000). The port is not published
on the host. `0` does not start the server. Each service owns a `metrics`
module and a private `CollectorRegistry`. `libs/schemas` stays a contract
package and does not gain a metrics helper.

The feature package has two registries. The API process and the worker process
expose only the series they record.

API latency histograms use buckets
`0.5 / 1 / 2 / 3 / 5 / 7.5 / 10 / 15 / 20 / 30 / 50 / 100 / 250` ms. The 5 ms
and 20 ms bounds are the K1 thresholds. `features_event_to_online_seconds` and
`detection_event_to_alert_seconds` use buckets
`0.01 / 0.05 / 0.1 / 0.25 / 0.5 / 1 / 2 / 5 / 10 / 30` s. The 2 s bound is the
K2 threshold.

Labels stay low-cardinality. A label value is never a `card_token`,
`merchant_id`, `event_id`, `request_id`, raw request path, or API key. The
gateway `route` label is the route template. `detection_alerts_fired_total`
uses the rule name. `gateway_rate_limited_total` uses the client name.

`detection_snapshots_total` uses `applied` when a snapshot was evaluated and
`skipped` when the payload was invalid. `gateway_upstream_errors_total` counts
each transport failure inside the proxy retry loop: `timeout` for
`httpx.TimeoutException`, `connect` for any other transport error.

## Alternatives considered

- **`/metrics` on the service port** — the worker has nothing to mount it on,
  and the gateway would have to exempt metrics from the API key on a published
  port.
- **One metrics module in `libs/schemas`** — the contract package would depend
  on a client library and on service-specific series.
- **The process-global `REGISTRY`** — scrapes would include default process
  collectors, and tests in one interpreter would share counters.

## Consequences

- Prometheus and Grafana are not part of this decision. A later scrape config
  reads port 9000 on the compose network.
- ClickHouse already publishes host port 9000. Service metrics stay reachable
  only from inside the compose network.
- A caught-up partition reports lag `0`. That gauge is still the lag series.
- Traces and the degradation journal are unchanged.
