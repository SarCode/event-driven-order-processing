.PHONY: test lint up down smoke build kind-load deploy-k8s smoke-k8s grafana

test:
	cd services/order-service && .venv/bin/python -m pytest tests -v
	cd services/workers && .venv/bin/python -m pytest tests -v

lint:
	services/order-service/.venv/bin/ruff check .

up:
	docker compose -f deploy/compose/docker-compose.yaml up --build -d

down:
	docker compose -f deploy/compose/docker-compose.yaml down -v

smoke:
	./scripts/smoke.sh

build:
	docker build -t orders/order-service:dev services/order-service
	docker build -t orders/workers:dev services/workers

kind-load: build
	kind load docker-image orders/order-service:dev --name orders
	kind load docker-image orders/workers:dev --name orders

deploy-k8s:
	kubectl apply -R -f deploy/k8s/

smoke-k8s:
	kubectl -n orders rollout status deploy/order-service --timeout=120s
	kubectl -n orders rollout status deploy/outbox-relay --timeout=120s
	kubectl -n orders rollout status deploy/status-consumer --timeout=120s
	kubectl -n orders rollout status deploy/inventory-worker --timeout=120s
	kubectl -n orders rollout status deploy/payment-worker --timeout=120s
	kubectl -n orders rollout status deploy/notification-worker --timeout=120s
	kubectl -n orders port-forward svc/order-service 8000:8000 & \
	sleep 3 && ./scripts/smoke.sh; \
	kill %1

grafana:
	kubectl -n monitoring port-forward svc/kps-grafana 3000:80
