# 0004: Bitnami OCI charts + bitnamilegacy images as a stopgap

## Status

Accepted, revisit before any non-local use

## Context

This project uses Bitnami's Helm charts for RabbitMQ and PostgreSQL. In
August 2025, Bitnami moved chart hosting to OCI registries and put
currently-maintained container images behind Broadcom's paid registry,
breaking the free `docker.io/bitnami/*` images that the charts pulled by
default. Applying the charts unmodified after that change fails to pull
images.

## Decision

Pull charts from `oci://registry-1.docker.io/bitnamicharts` (the still-free
OCI chart location) and override `image.repository` to the
`bitnamilegacy/*` mirrors, which host the last free images, with
`global.security.allowInsecureImages=true` set because the legacy images are
unsigned/unverified by the chart's normal checks. Chart versions are pinned
explicitly in `terraform/main.tf` (RabbitMQ 16.0.14, PostgreSQL 18.7.11) so
the workaround doesn't silently break again on an unrelated version bump.

## Consequences

- The project keeps running on free infrastructure with no code changes
  beyond Terraform `set` blocks and version pins.
- `bitnamilegacy` images are explicitly a stopgap: Bitnami/Broadcom give no
  update guarantee, so these images will not receive security patches going
  forward.
- `allowInsecureImages=true` disables a safety check; acceptable for a local
  dev/demo cluster, not acceptable to carry into any shared or
  internet-facing environment.
- Pinned versions mean this repo will not silently pick up Bitnami's next
  breaking change, but it also will not pick up fixes - someone must
  periodically re-evaluate whether a non-Bitnami chart (e.g. the RabbitMQ or
  CloudNativePG community charts) should replace this before the project is
  used for anything beyond local demonstration.
