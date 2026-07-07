variable "app_password" {
  description = "Password for the app's Postgres and RabbitMQ users. Dev default; override with TF_VAR_app_password for anything non-local."
  type        = string
  default     = "orders-dev-password"
  sensitive   = true
}
