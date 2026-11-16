COMPOSE := docker compose -f infra/docker-compose.yml
KAFKA_BOOTSTRAP := localhost:9092
KAFKA_TOPICS := /opt/kafka/bin/kafka-topics.sh

.PHONY: up down logs ps topics secret lint typecheck test test-int test-e2e clean openapi pan-scan obs-up

up:
	$(COMPOSE) up -d --wait kafka postgres redis clickhouse
	$(MAKE) topics
	$(COMPOSE) up -d --wait ingestion features-worker features-api detection gateway

obs-up:
	$(COMPOSE) --profile obs up -d --wait prometheus

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

topics:
	$(COMPOSE) exec -T kafka $(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) \
		--create --if-not-exists --topic transactions.raw --partitions 4 --replication-factor 1 \
		--config retention.ms=-1
	$(COMPOSE) exec -T kafka $(KAFKA_TOPICS) --bootstrap-server $(KAFKA_BOOTSTRAP) \
		--create --if-not-exists --topic features.updates --partitions 4 --replication-factor 1 \
		--config retention.ms=604800000

# ADR-9: HMAC key for PAN hashing. Do not overwrite an existing key.
secret:
	mkdir -p secrets
	test -f secrets/hmac_key || openssl rand -hex 32 > secrets/hmac_key

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

clean:
	$(COMPOSE) down -v
