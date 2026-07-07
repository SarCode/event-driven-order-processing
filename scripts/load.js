import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  stages: [
    { duration: "30s", target: 10 },
    { duration: "1m", target: 10 },
    { duration: "15s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<500"],
    checks: ["rate>0.99"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";

export default function () {
  const quantity = Math.floor(Math.random() * 3) + 1;
  const res = http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({ sku: "ABC-1", quantity: quantity }),
    { headers: { "Content-Type": "application/json" } },
  );
  check(res, { "status 201": (r) => r.status === 201 });
  sleep(1);
}
