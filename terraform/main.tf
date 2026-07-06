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
  namespace  = kubernetes_namespace.orders.metadata[0].name

  set {
    name  = "auth.username"
    value = "orders"
  }
  set {
    name  = "auth.password"
    value = "orders-dev-password"
  }
  set {
    name  = "image.repository"
    value = "bitnamilegacy/rabbitmq"
  }
  set {
    name  = "global.security.allowInsecureImages"
    value = "true"
  }
}

resource "helm_release" "postgresql" {
  name       = "postgres"
  repository = "oci://registry-1.docker.io/bitnamicharts"
  chart      = "postgresql"
  namespace  = kubernetes_namespace.orders.metadata[0].name

  set {
    name  = "auth.username"
    value = "orders"
  }
  set {
    name  = "auth.password"
    value = "orders-dev-password"
  }
  set {
    name  = "auth.database"
    value = "orders"
  }
  set {
    name  = "image.repository"
    value = "bitnamilegacy/postgresql"
  }
  set {
    name  = "global.security.allowInsecureImages"
    value = "true"
  }
}
