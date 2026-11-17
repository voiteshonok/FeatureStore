import http from "k6/http";
import { check, fail } from "k6";
import { SharedArray } from "k6/data";

const CATALOG = [
  "card_cnt_1m",
  "card_cnt_1h",
  "card_cnt_24h",
  "card_sum_1m",
  "card_sum_1h",
  "card_sum_24h",
  "geo_velocity_kmh",
  "merchant_cnt_24h",
  "merchant_decline_rate_24h",
];

const TARGETS = {
  direct: { port: 8001, auth: false },
  gateway: { port: 8080, auth: true },
  baseline: { port: 8003, auth: false },
};

const smoke = __ENV.SMOKE === "1";
const targetName = __ENV.TARGET || "";
if (!(targetName in TARGETS)) {
  fail(`TARGET must be one of: ${Object.keys(TARGETS).join(", ")}`);
}
const target = TARGETS[targetName];

const host = __ENV.HOST || "localhost";
const apiKey = __ENV.API_KEY || "dev-scoring-key";
const rate = smoke ? 20 : Number(__ENV.RATE || 200);
const duration = smoke ? "15s" : __ENV.DURATION || "10m";
const tokensFile = __ENV.TOKENS_FILE || "experiments/load/tokens.json";

const tokens = new SharedArray("tokens", () => {
  const raw = open(tokensFile);
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error(`${tokensFile} must be a non-empty JSON array`);
  }
  return parsed;
});

const preAllocatedVUs = Math.max(rate, 50);
const maxVUs = Math.max(rate * 2, 100);

const thresholds = {
  http_req_failed: ["rate<0.001"],
  checks: ["rate>0.999"],
};
if (!smoke && targetName !== "baseline") {
  thresholds.http_req_duration = ["p(99)<20", "p(50)<5"];
}

export const options = {
  scenarios: {
    features: {
      executor: "constant-arrival-rate",
      rate,
      timeUnit: "1s",
      duration,
      preAllocatedVUs,
      maxVUs,
    },
  },
  thresholds,
};

const url = `http://${host}:${target.port}/v1/features`;

function pickKnown() {
  return tokens[Math.floor(Math.random() * tokens.length)];
}

function pickUnknown() {
  const id = `${__VU}-${__ITER}-${Date.now()}`;
  return {
    card_token: `unknown-card-${id}`,
    merchant_id: `unknown-merchant-${id}`,
    lat: 52.52,
    lon: 13.405,
  };
}

export default function () {
  const row = Math.random() < 0.95 ? pickKnown() : pickUnknown();
  const headers = { "Content-Type": "application/json" };
  if (target.auth) {
    headers["X-API-Key"] = apiKey;
  }
  const body = JSON.stringify({
    card_token: row.card_token,
    merchant_id: row.merchant_id,
    ts: new Date().toISOString(),
    lat: row.lat,
    lon: row.lon,
  });
  const res = http.post(url, body, { headers });
  check(res, {
    "status is 200": (r) => r.status === 200,
    "catalog features present": (r) => {
      if (r.status !== 200) {
        return false;
      }
      let payload;
      try {
        payload = r.json();
      } catch (_) {
        return false;
      }
      const features = payload && payload.features;
      if (!features || typeof features !== "object") {
        return false;
      }
      for (const name of CATALOG) {
        const entry = features[name];
        if (!entry || typeof entry !== "object" || entry.status === undefined) {
          return false;
        }
      }
      return true;
    },
  });
}
