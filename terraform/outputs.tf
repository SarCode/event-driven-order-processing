output "cluster_name" {
  value = kind_cluster.orders.name
}

output "kubeconfig_context" {
  value = "kind-${kind_cluster.orders.name}"
}
