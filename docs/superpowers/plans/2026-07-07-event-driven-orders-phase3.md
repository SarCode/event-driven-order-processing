# Event-Driven Order Processing - Phase 3 (CI/CD, GitHub, Secrets, Hardening) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Public GitHub repo with working CI/CD: lint + tests + two end-to-end jobs (compose and terraform-provisioned ephemeral kind) on every PR/push, images published to GHCR on main, credentials moved to a Kubernetes Secret, probes and resource limits on deployments.

**Architecture:** Two GitHub Actions workflows. `ci.yml`: lint (ruff) + unit tests (matrix over both services) gate two e2e jobs - `e2e-compose` (fast path: compose up + saga smoke) and `e2e-kind` (the portfolio piece: the SAME terraform config used locally provisions a kind cluster inside the runner, images built + kind-loaded, manifests applied, saga smoke). `cd.yml` on main: build + push both images to GHCR tagged `sha` + `latest`. Credentials: Terraform gains an `app_password` variable (dev default) feeding both Helm releases and a new `kubernetes_secret` (`app-credentials` with DATABASE_URL/RABBITMQ_URL); manifests switch inline env to `envFrom: secretRef`. order-service gains a liveness probe; all six deployments gain resource requests/limits.

**Tech Stack additions:** GitHub Actions, GHCR, ruff. No new app code.

**Known constraints:** GitHub repo creation/push (Task 6) requires `gh auth login` by the user - execute Tasks 1-5 + 7 locally first if auth is missing. Bitnami image pulls in the kind CI job take minutes; job gets a 20 minute timeout.

**Working directory:** repo root `/Users/sarthakagarwal/Documents/Claude/Projects/AWS Whitepaper`. Environment: docker CLI via `export PATH="/usr/local/bin:$PATH"`; kind + terraform + gh at /opt/homebrew/bin; kind cluster "orders" live with full Phase 2 stack.

---

## File Structure

```
.github/workflows/ci.yml    # NEW
.github/workflows/cd.yml    # NEW
ruff.toml                   # NEW (root lint config)
terraform/main.tf           # MODIFIED: app_password var, kubernetes_secret
terraform/variables.tf      # NEW
deploy/k8s/order-service.yaml   # MODIFIED: envFrom secret, liveness, resources
deploy/k8s/order-workers.yaml   # MODIFIED: envFrom secret, resources
deploy/k8s/workers.yaml         # MODIFIED: envFrom secret, resources
Makefile                    # MODIFIED: lint target
services/*/requirements-dev.txt # MODIFIED: + ruff
README.md                   # MODIFIED: badges, CI/CD section, GHCR
```

---

### Task 1: Kubernetes Secret via Terraform + manifests envFrom

**Files:**
- Create: `terraform/variables.tf`
- Modify: `terraform/main.tf`
- Modify: `deploy/k8s/order-service.yaml`, `deploy/k8s/order-workers.yaml`, `deploy/k8s/workers.yaml`

- [ ] **Step 1: Create `terraform/variables.tf`**

```hcl
variable "app_password" {
  description = "Password for the app's Postgres and RabbitMQ users. Dev default; override with TF_VAR_app_password for anything non-local."
  type        = string
  default     = "orders-dev-password"
  sensitive   = true
}
```

- [ ] **Step 2: Modify `terraform/main.tf`**

Replace the two hardcoded `value = "orders-dev-password"` occurrences in the helm_release set blocks with `value = var.app_password`, and add after the postgresql release:

```hcl
resource "kubernetes_secret" "app_credentials" {
  metadata {
    name      = "app-credentials"
    namespace = kubernetes_namespace.orders.metadata[0].name
  }

  data = {
    DATABASE_URL = "postgresql://orders:${var.app_password}@postgres-postgresql:5432/orders"
    RABBITMQ_URL = "amqp://orders:${var.app_password}@rabbitmq:5672/%2F"
  }
}
```

- [ ] **Step 3: Apply and verify**

```bash
cd terraform && terraform validate && terraform apply -auto-approve
kubectl --context kind-orders -n orders get secret app-credentials
```
Expected: apply adds exactly 1 resource (the secret; helm releases unchanged since the var default matches the old literal). Secret exists with 2 data keys.

- [ ] **Step 4: Switch all six deployments to envFrom**

In `deploy/k8s/order-service.yaml`, `deploy/k8s/order-workers.yaml`, `deploy/k8s/workers.yaml`: delete every `env:` block (with its DATABASE_URL / RABBITMQ_URL entries) and replace with:

```yaml
          envFrom:
            - secretRef:
                name: app-credentials
```

(order-service only reads DATABASE_URL; the extra RABBITMQ_URL key via envFrom is harmless.)

- [ ] **Step 5: Redeploy and verify saga still works**

```bash
export PATH="/usr/local/bin:$PATH"
make deploy-k8s
kubectl --context kind-orders -n orders rollout restart deploy order-service outbox-relay status-consumer inventory-worker payment-worker notification-worker
make smoke-k8s
```
Expected: `smoke ok`.

- [ ] **Step 6: Commit**

```bash
git add terraform/ deploy/k8s/
git commit -m "feat(k8s): move credentials to a terraform-managed secret"
```

---

### Task 2: Probes and resource limits

**Files:**
- Modify: `deploy/k8s/order-service.yaml`, `deploy/k8s/order-workers.yaml`, `deploy/k8s/workers.yaml`

- [ ] **Step 1: order-service liveness probe** (add below the existing readinessProbe, same indentation)

```yaml
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 10
```

- [ ] **Step 2: resources on ALL six containers** (order-service, outbox-relay, status-consumer, inventory-worker, payment-worker, notification-worker - same block each, placed after env/envFrom)

```yaml
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 250m
              memory: 256Mi
```

Workers/relay/status-consumer get no liveness probe: they have no HTTP surface; a wedged consume loop exits on error and the pod restart policy recovers it. Add this as a YAML comment above the first worker deployment in workers.yaml:

```yaml
# No liveness probes on consumers: no HTTP surface to probe. Failure modes
# exit the process (see runtime.py reconnect handling) and Kubernetes restarts.
```

- [ ] **Step 3: Redeploy + verify**

```bash
make deploy-k8s
kubectl --context kind-orders -n orders get pods
make smoke-k8s
```
Expected: all pods reach Running 1/1 with new spec; `smoke ok`.

- [ ] **Step 4: Commit**

```bash
git add deploy/k8s/
git commit -m "feat(k8s): liveness probe and resource limits on app deployments"
```

---

### Task 3: Ruff lint

**Files:**
- Create: `ruff.toml` (repo root)
- Modify: `services/order-service/requirements-dev.txt`, `services/workers/requirements-dev.txt`
- Modify: `Makefile`

- [ ] **Step 1: Create `ruff.toml`**

```toml
line-length = 100
target-version = "py312"

[lint]
select = ["E", "F", "W", "I"]
```

- [ ] **Step 2: Add `ruff>=0.4` to both requirements-dev.txt files and install**

```bash
cd services/order-service && .venv/bin/pip install ruff && cd ../workers && .venv/bin/pip install ruff
```

- [ ] **Step 3: Makefile lint target** (add after `test`)

```makefile
lint:
	services/order-service/.venv/bin/ruff check .
```
Add `lint` to the `.PHONY` line.

- [ ] **Step 4: Run and fix**

```bash
make lint
```
Fix any violations (`ruff check --fix .` for auto-fixable, manual for the rest), then `make test` to confirm 29 still pass. Report what needed fixing.

- [ ] **Step 5: Commit**

```bash
git add ruff.toml Makefile services/*/requirements-dev.txt <any fixed files>
git commit -m "feat: ruff lint config with make target"
```

---

### Task 4: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: ci

on:
  pull_request:
  push:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install ruff
      - run: ruff check .

  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        service: [order-service, workers]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install and test
        working-directory: services/${{ matrix.service }}
        run: |
          pip install -r requirements-dev.txt
          python -m pytest tests -v

  e2e-compose:
    runs-on: ubuntu-latest
    needs: [lint, test]
    steps:
      - uses: actions/checkout@v4
      - name: Start stack
        run: docker compose -f deploy/compose/docker-compose.yaml up --build -d
      - name: Wait for services
        run: sleep 25
      - name: Saga smoke test
        run: ./scripts/smoke.sh
      - name: Logs on failure
        if: failure()
        run: docker compose -f deploy/compose/docker-compose.yaml logs
      - name: Tear down
        if: always()
        run: docker compose -f deploy/compose/docker-compose.yaml down -v

  e2e-kind:
    runs-on: ubuntu-latest
    needs: [lint, test]
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3
        with:
          terraform_wrapper: false
      - name: Install kind
        run: |
          curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.23.0/kind-linux-amd64
          chmod +x ./kind
          sudo mv ./kind /usr/local/bin/kind
      - name: Provision cluster and infra (same terraform as local)
        working-directory: terraform
        run: |
          terraform init
          terraform apply -auto-approve
      - name: Build images and load into kind
        run: make kind-load
      - name: Deploy app manifests
        run: make deploy-k8s
      - name: Saga smoke test in-cluster
        run: make smoke-k8s
      - name: Diagnostics on failure
        if: failure()
        run: |
          kubectl -n orders get pods || true
          kubectl -n orders logs deploy/order-service --tail=50 || true
          kubectl -n orders logs deploy/outbox-relay --tail=50 || true
          kubectl -n orders logs deploy/inventory-worker --tail=50 || true
```

- [ ] **Step 2: Static validation** (no Actions runner locally)

```bash
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('yaml ok')"
```
Also self-check: every `make` target referenced exists; smoke-k8s's kubectl calls use the default context (kind sets it in the runner).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "feat(ci): lint, tests, compose and ephemeral-kind e2e jobs"
```

---

### Task 5: CD workflow (GHCR publish)

**Files:**
- Create: `.github/workflows/cd.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: cd

on:
  push:
    branches: [main]

permissions:
  contents: read
  packages: write

jobs:
  publish-images:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - name: Build and push both images
        run: |
          repo=$(echo "ghcr.io/${{ github.repository }}" | tr '[:upper:]' '[:lower:]')
          for svc in order-service workers; do
            docker build -t "$repo/$svc:${{ github.sha }}" -t "$repo/$svc:latest" "services/$svc"
            docker push "$repo/$svc:${{ github.sha }}"
            docker push "$repo/$svc:latest"
          done
```

- [ ] **Step 2: Validate YAML** (same python one-liner) **and commit**

```bash
git add .github/workflows/cd.yml
git commit -m "feat(cd): publish images to ghcr on main"
```

---

### Task 6: GitHub repo + push (REQUIRES `gh auth login` BY USER)

- [ ] **Step 1: Verify auth**: `gh auth status` - if not logged in, STOP and request user action.
- [ ] **Step 2: Create repo and push**

```bash
cd "/Users/sarthakagarwal/Documents/Claude/Projects/AWS Whitepaper"
gh repo create event-driven-order-processing --public --source=. --remote=origin --push
```

- [ ] **Step 3: Verify Actions**

```bash
gh run list --limit 4
gh run watch <ci-run-id> --exit-status
```
Expected: ci (all 4 jobs) and cd both green. If a job fails, read `gh run view <id> --log-failed`, fix, commit, push, re-verify.

---

### Task 7: README v3

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add badges under the title** (after Task 6 so OWNER/REPO known; if Task 6 still blocked, use placeholder `OWNER/event-driven-order-processing` and note it)

```markdown
![ci](https://github.com/OWNER/event-driven-order-processing/actions/workflows/ci.yml/badge.svg)
![cd](https://github.com/OWNER/event-driven-order-processing/actions/workflows/cd.yml/badge.svg)
```

- [ ] **Step 2: Add CI/CD section** (after "Resilience patterns")

```markdown
## CI/CD

Every push and pull request runs [ci.yml](.github/workflows/ci.yml):

1. **lint** - ruff over both services
2. **test** - pytest matrix (order-service, workers)
3. **e2e-compose** - full stack via Docker Compose + saga smoke test
4. **e2e-kind** - the same Terraform config used locally provisions an
   ephemeral kind cluster inside the runner; images are built, loaded, and
   the saga smoke test runs in-cluster

On main, [cd.yml](.github/workflows/cd.yml) publishes both images to GHCR
(`ghcr.io/OWNER/event-driven-order-processing/{order-service,workers}`)
tagged with the commit SHA and `latest`.

Credentials live in a Terraform-managed Kubernetes Secret (`app-credentials`);
the dev default is overridable via `TF_VAR_app_password`.
```

- [ ] **Step 3: Update Roadmap** (remove Phase 3 line) **and commit**

```bash
git add README.md
git commit -m "docs: phase 3 README with CI/CD pipeline and badges"
```

---

## Verification checklist (end of Phase 3)

- [ ] `make lint` clean, `make test` 29 passed
- [ ] Secret-based creds: `kubectl -n orders get secret app-credentials` exists, saga smoke green after envFrom switch
- [ ] Probes/resources live on all six deployments
- [ ] GitHub: repo public, ci.yml all 4 jobs green, cd.yml pushed images visible in GHCR
- [ ] README badges render, roadmap shows only Phase 4

## Follow-up plan

Phase 4: `2026-07-XX-phase4-observability.md` - kube-prometheus-stack via terraform, app metrics endpoints, Grafana dashboards, alert rules, k6, chaos experiment, ADRs
