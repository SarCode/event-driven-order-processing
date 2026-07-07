#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"

wait_for_status() {
  order_id="$1"
  want="$2"
  status=""
  for _ in $(seq 1 30); do
    status=$(curl -sf "$BASE_URL/orders/$order_id" | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
    if [ "$status" = "$want" ]; then
      echo "order $order_id reached $want"
      return 0
    fi
    sleep 1
  done
  echo "order $order_id stuck at '$status' (wanted $want)"
  return 1
}

create_order() {
  curl -sf -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"sku\": \"$1\", \"quantity\": $2}" |
    grep -o '"order_id":"[^"]*"' | cut -d'"' -f4
}

echo "checking health..."
curl -sf "$BASE_URL/healthz" | grep -q '"status":"ok"'

echo "happy path: small order should confirm..."
id=$(create_order ABC-1 2)
wait_for_status "$id" confirmed

echo "payment failure path: big order should reject with compensation..."
id=$(create_order ABC-1 60)
wait_for_status "$id" rejected

echo "inventory failure path: order beyond stock should reject..."
id=$(create_order XYZ-9 99)
wait_for_status "$id" rejected

echo "smoke ok"
