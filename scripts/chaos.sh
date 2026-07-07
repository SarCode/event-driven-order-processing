#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
SKU="ABC-1"
ORDER_COUNT=10
TIMEOUT_SECS=120

create_order() {
  curl -sf -X POST "$BASE_URL/orders" \
    -H 'Content-Type: application/json' \
    -d "{\"sku\": \"$1\", \"quantity\": $2}" |
    grep -o '"order_id":"[^"]*"' | cut -d'"' -f4
}

get_status() {
  order_id="$1"
  # || true keeps transient curl/grep failures (port-forward hiccups,
  # service still starting) inside the retry loop instead of killing
  # the whole script via set -e.
  curl -sf "$BASE_URL/orders/$order_id" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 || true
}

wait_for_terminal() {
  order_id="$1"
  status=""
  for _ in $(seq 1 "$TIMEOUT_SECS"); do
    status=$(get_status "$order_id")
    if [ "$status" = "confirmed" ] || [ "$status" = "rejected" ]; then
      echo "$status"
      return 0
    fi
    sleep 1
  done
  echo "${status:-pending}"
  return 1
}

available_stock() {
  kubectl -n orders exec postgres-postgresql-0 -- env PGPASSWORD=orders-dev-password \
    psql -U orders -d orders -t -A -c "SELECT available FROM inventory WHERE sku = '$SKU';"
}

echo "checking health..."
curl -sf "$BASE_URL/healthz" | grep -q '"status":"ok"'

echo "recording stock before..."
stock_before=$(available_stock)
echo "stock before: $stock_before"

echo "posting $ORDER_COUNT orders ($SKU, quantity 1)..."
order_ids=()
for _ in $(seq 1 "$ORDER_COUNT"); do
  id=$(create_order "$SKU" 1)
  order_ids+=("$id")
done
echo "created ${#order_ids[@]} orders: ${order_ids[*]}"

echo "killing inventory-worker pod mid-stream..."
kubectl -n orders delete pod -l app=inventory-worker

echo "polling orders for terminal status (timeout ${TIMEOUT_SECS}s each)..."
statuses=()
failures=0
for id in "${order_ids[@]}"; do
  if status=$(wait_for_terminal "$id"); then
    statuses+=("$status")
  else
    statuses+=("$status")
    failures=$((failures + 1))
  fi
done

echo "recording stock after..."
stock_after=$(available_stock)

confirmed=0
rejected=0
for s in "${statuses[@]}"; do
  case "$s" in
    confirmed) confirmed=$((confirmed + 1)) ;;
    rejected) rejected=$((rejected + 1)) ;;
  esac
done

expected_after=$((stock_before - confirmed))

echo ""
echo "=== chaos experiment summary ==="
printf '%-40s %-12s\n' "order_id" "status"
printf '%-40s %-12s\n' "----------------------------------------" "------------"
for i in "${!order_ids[@]}"; do
  printf '%-40s %-12s\n' "${order_ids[$i]}" "${statuses[$i]}"
done
echo "---------------------------------------------"
echo "terminal: $((confirmed + rejected))/$ORDER_COUNT (confirmed=$confirmed, rejected=$rejected)"
echo "stock before: $stock_before"
echo "stock after:  $stock_after"
echo "expected after (before - confirmed units): $expected_after"
echo "==============================================="

if [ "$failures" -ne 0 ]; then
  echo "FAIL: $failures order(s) did not reach terminal status within ${TIMEOUT_SECS}s"
  exit 1
fi

if [ "$stock_after" -ne "$expected_after" ]; then
  echo "FAIL: stock math violated (available=$stock_after, expected=$expected_after)"
  exit 1
fi

echo "chaos experiment ok: all orders terminal, stock math holds"
