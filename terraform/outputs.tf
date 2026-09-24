output "kafka_bootstrap_service" {
  description = "In-cluster Kafka bootstrap address"
  value       = "${kubernetes_service_v1.kafka_bootstrap.metadata[0].name}.${var.namespace}.svc.cluster.local:9092"
}

output "broker_replicas" {
  description = "Terraform desired broker count"
  value       = kubernetes_stateful_set_v1.kafka.spec[0].replicas
}

