COMPOSE := docker compose -f infra/docker-compose.yml
# `up` does not enable profiles. `down` must, or those containers keep the network.
PROFILES := --profile baseline --profile sim
KAFKA_BOOTSTRAP := localhost:9092
KAFKA_TOPICS := /opt/kafka/bin/kafka-topics.sh

.PHONY: up down logs ps topics secret lint typecheck test test-int test-e2e clean openapi pan-scan audit obs-up baseline-up backfill demo exp-k1k2 exp-k1k2-short exp-ladder

up:
	$(COMPOSE) up -d --wait kafka postgres redis clickhouse
	$(MAKE) topics
	$(COMPOSE) up -d --wait ingestion features-worker features-offline features-api detection prometheus gateway

obs-up:
	$(COMPOSE) up -d --wait prometheus

baseline-up:
	$(COMPOSE) --profile baseline up -d --wait baseline-writer baseline-api

# One-shot history fill. Does not start with --profile sim.
# 3m at 200 TPS is 36,000 primary events: about 5 minutes at ~130 posts/s.
backfill:
	$(COMPOSE) --profile sim run --build --rm simulator \
		--backfill $(or $(BACKFILL),3m) \
		--tps $(or $(TPS),200) \
		--send-rate $(or $(SEND_RATE),2000) \
		--seed $(or $(SEED),42) \
		--run-id $(or $(RUN_ID),warm) \
		--url http://ingestion:8000

# Live demo traffic with anomaly scenarios (card burst + bad merchants).
demo: up
	$(COMPOSE) --profile sim run --build --rm simulator \
		--tps 50 \
		--duration 5m \
		--seed 42 \
		--anomalies \
		--url http://ingestion:8000

# First K1/K2 + Redis memory pack → experiments/results/<date>-k1-k2/
# Default warm-up BACKFILL=30s (fast catch-up). Override BACKFILL=3m or 23h as needed.
exp-k1k2:
	PATH="$(HOME)/.local/bin:$(PATH)" PYTHONPATH=experiments/k1_k2 \
		uv run python experiments/k1_k2/run.py

exp-k1k2-short:
	PATH="$(HOME)/.local/bin:$(PATH)" MODE=short PYTHONPATH=experiments/k1_k2 \
		uv run python experiments/k1_k2/run.py

# Direct-only read ladder 20/50/100/200 × 2m → experiments/results/<date>-ladder/
exp-ladder:
	PATH="$(HOME)/.local/bin:$(PATH)" PYTHONPATH=experiments/k1_k2 \
		uv run python experiments/k1_k2/ladder.py

down:
	$(COMPOSE) $(PROFILES) down

logs:
	$(COMPOSE) $(PROFILES) logs -f --tail=100

ps:
	$(COMPOSE) $(PROFILES) ps

topics:
	$(COMPOSE) exec -T kafka $(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) \
		--create --if-not-exists --topic transactions.raw --partitions 4 --replication-factor 1 \
		--config retention.ms=-1
	$(COMPOSE) exec -T kafka $(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) \
		--create --if-not-exists --topic features.updates --partitions 4 --replication-factor 1 \
		--config retention.ms=604800000

# ADR-9 / ADR-020: HMAC and JWT secrets. Do not overwrite existing files.
secret:
	mkdir -p secrets
	test -f secrets/hmac_key || openssl rand -hex 32 > secrets/hmac_key
	test -f secrets/jwt_secret || openssl rand -hex 32 > secrets/jwt_secret

lint:
	uv run ruff check .

typecheck:
	uv run mypy .

test:
	uv run pytest -m "not integration and not e2e"

test-int:
	uv run pytest -m integration

test-e2e:
	uv run pytest -m e2e --no-cov

openapi:
	uv run python -c "from ingestion.app import export_openapi; export_openapi('docs/openapi/ingestion.json')"
	uv run python -c "from features.api import export_openapi; export_openapi('docs/openapi/features.json')"
	uv run python -c "from detection.api import export_openapi; export_openapi('docs/openapi/detection.json')"
	uv run python -c "from gateway.api import export_openapi; export_openapi('docs/openapi/gateway.json')"

pan-scan:
	PYTHONPATH=tools uv run python -m pan_scan

audit:
	uv export --frozen --no-dev --no-emit-workspace -o /tmp/featurestore-reqs.txt
	uvx pip-audit -r /tmp/featurestore-reqs.txt

clean:
	$(COMPOSE) $(PROFILES) down -v
