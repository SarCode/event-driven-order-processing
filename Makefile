.PHONY: test up down smoke build kind-load deploy-k8s smoke-k8s

test:
	cd services/order-service && .venv/bin/python -m pytest tests -v
	cd services/inventory-worker && .venv/bin/python -m pytest tests -v

up:
	docker compose -f deploy/compose/docker-compose.yaml up --build -d

down:
	docker compose -f deploy/compose/docker-compose.yaml down -v

smoke:
	./scripts/smoke.sh

build:
	docker build -t orders/order-service:dev services/order-service
	docker build -t orders/inventory-worker:dev services/inventory-worker

kind-load: build
	kind load docker-image orders/order-service:dev --name orders
	kind load docker-image orders/inventory-worker:dev --name orders

deploy-k8s:
	kubectl apply -f deploy/k8s/

smoke-k8s:
	kubectl -n orders rollout status deploy/order-service --timeout=120s
	kubectl -n orders port-forward svc/order-service 8000:8000 & \
	sleep 3 && ./scripts/smoke.sh; \
	kill %1
