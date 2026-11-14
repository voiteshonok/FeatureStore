COMPOSE := docker compose -f infra/docker-compose.yml
KAFKA_BOOTSTRAP := localhost:9092
KAFKA_TOPICS := /opt/kafka/bin/kafka-topics.sh

.PHONY: up down logs ps topics secret lint typecheck test test-int clean openapi

up:
	$(COMPOSE) up -d --wait kafka postgres redis clickhouse
	$(MAKE) topics
	$(COMPOSE) up -d --wait ingestion

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
	uv run pytest -m "not integration"

test-int:
	uv run pytest -m integration

openapi:
	uv run python -c "from ingestion.app import export_openapi; export_openapi('docs/openapi/ingestion.json')"

clean:
	$(COMPOSE) down -v
