provider "kind" {}

resource "kind_cluster" "orders" {
  name           = "orders"
  wait_for_ready = true
}

provider "kubernetes" {
  host                   = kind_cluster.orders.endpoint
  client_certificate     = kind_cluster.orders.client_certificate
  client_key             = kind_cluster.orders.client_key
  cluster_ca_certificate = kind_cluster.orders.cluster_ca_certificate
}

provider "helm" {
  kubernetes {
    host                   = kind_cluster.orders.endpoint
    client_certificate     = kind_cluster.orders.client_certificate
    client_key             = kind_cluster.orders.client_key
    cluster_ca_certificate = kind_cluster.orders.cluster_ca_certificate
  }
}

resource "kubernetes_namespace" "orders" {
  metadata {
    name = "orders"
  }
}

resource "helm_release" "rabbitmq" {
  name       = "rabbitmq"
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "rabbitmq"
  version    = "16.0.14"
  namespace  = kubernetes_namespace.orders.metadata[0].name

  set {
    name  = "auth.username"
    value = "orders"
  }
  set {
    name  = "auth.password"
    value = var.app_password
  }
  # Bitnami moved charts to OCI and put current images behind Broadcom's paid registry (Aug 2025).
  # bitnamilegacy images are a temporary free stopgap; revisit before any non-local use.
  set {
    name  = "image.repository"
    value = "bitnamilegacy/rabbitmq"
  }
  set {
    name  = "global.security.allowInsecureImages"
    value = "true"
  }
  set {
    name  = "metrics.enabled"
    value = "true"
  }
  set {
    name  = "metrics.serviceMonitor.enabled"
    value = "true"
  }
  # Per-queue labels (queue="orders.dlq" etc) require per-object metrics;
  # the default endpoint only aggregates. Dashboard and DLQ alert depend on this.
  set {
    name  = "extraConfiguration"
    value = "prometheus.return_per_object_metrics = true"
  }
}

resource "helm_release" "postgresql" {
  name       = "postgres"
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "postgresql"
  version    = "18.7.11"
  namespace  = kubernetes_namespace.orders.metadata[0].name

  set {
    name  = "auth.username"
    value = "orders"
  }
  set {
    name  = "auth.password"
    value = var.app_password
  }
  set {
    name  = "auth.database"
    value = "orders"
  }
  # Bitnami moved charts to OCI and put current images behind Broadcom's paid registry (Aug 2025).
  # bitnamilegacy images are a temporary free stopgap; revisit before any non-local use.
  set {
    name  = "image.repository"
    value = "bitnamilegacy/postgresql"
  }
  set {
    name  = "global.security.allowInsecureImages"
    value = "true"
  }
}

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

resource "kubernetes_namespace" "monitoring" {
  metadata {
    name = "monitoring"
  }
}

resource "helm_release" "kube_prometheus_stack" {
  name       = "kps"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  version    = "62.7.0"
  namespace  = kubernetes_namespace.monitoring.metadata[0].name
  timeout    = 900

  # Deliberate dev shortcut: one shared password across postgres, rabbitmq,
  # and grafana. Split credentials before any shared or long-lived environment.
  set {
    name  = "grafana.adminPassword"
    value = var.app_password
  }
  # Scrape PodMonitors/ServiceMonitors from all namespaces without label gating.
  # Fine for this single-tenant dev cluster; tighten with label selectors if
  # the cluster ever hosts anything else.
  set {
    name  = "prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues"
    value = "false"
  }
  set {
    name  = "prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues"
    value = "false"
  }
  set {
    name  = "prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues"
    value = "false"
  }
}
