# 0003: Local kind cluster over cloud EKS

## Status

Accepted (Phase 1, reaffirmed Phase 3)

## Context

The reference whitepaper architecture runs on managed AWS services (EKS for
compute, RDS, SNS/SQS). Standing up the equivalent on real AWS costs money
continuously (EKS control plane, RDS instance, NAT/load balancer hours) for a
project whose purpose is to demonstrate the architecture and its resilience
patterns, not to operate a production service. Phase 3 additionally needed a
cluster that CI could stand up and tear down per PR (ephemeral, no lingering
cost or state).

## Decision

Provision a local `kind` (Kubernetes in Docker) cluster with Terraform
instead of EKS, using the same Terraform configuration for both local
development and CI. GitHub Actions creates an ephemeral kind cluster per PR
run using this identical IaC, so "the infra a developer runs locally" and
"the infra CI validates against" are the same code path, not two
parallel definitions that can drift.

## Consequences

- Zero cloud cost for the entire project, and instant teardown/recreation
  locally or in CI.
- Because it is the same Terraform for local and CI, a passing CI run is
  meaningful evidence the local workflow still works, and vice versa.
- We do not get a real cloud IAM story, load balancer, or multi-node
  networking model - `kind` runs single-node-per-container on the developer's
  or runner's machine, so anything specific to EKS networking, IAM roles for
  service accounts, or an internet-facing load balancer is out of scope and
  unproven by this project.
- Anyone taking this further toward an actual deployment must budget
  separate work to validate cloud networking, IAM, and managed-service
  behavior that kind cannot exercise.
