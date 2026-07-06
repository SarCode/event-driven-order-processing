#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

echo "checking health..."
curl -sf "$BASE_URL/healthz" | grep -q '"status":"ok"'

echo "creating order..."
resp=$(curl -sf -X POST "$BASE_URL/orders" \
  -H 'Content-Type: application/json' \
  -d '{"sku": "ABC-1", "quantity": 2}')
echo "$resp"
echo "$resp" | grep -q '"status":"pending"'

echo "smoke ok"
