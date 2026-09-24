variable "kube_context" {
  description = "kubectl context containing the existing Kafka cluster"
  type        = string
  default     = null
}

variable "kube_config_path" {
  description = "Path to the kubeconfig containing kube_context"
  type        = string
  default     = "~/.kube/config"
}

variable "namespace" {
  description = "Namespace of the existing Kafka cluster"
  type        = string
  default     = "kafka"
}

variable "statefulset_name" {
  description = "Name of the Terraform-managed Kafka StatefulSet"
  type        = string
  default     = "kafka"
}

variable "broker_count" {
  description = "Desired broker count. The Python guard only permits changing 2 to 3."
  type        = number
  default     = 2

  validation {
    condition     = contains([2, 3], var.broker_count)
    error_message = "V1 models only the two-broker baseline and three-broker target."
  }
}

variable "kafka_image" {
  description = "Pinned Apache Kafka image used by the existing StatefulSet"
  type        = string
  default     = "apache/kafka:3.8.0"
}

variable "cluster_id" {
  description = "Existing KRaft cluster ID; provide via TF_VAR_cluster_id"
  type        = string
  sensitive   = true
}

variable "controller_quorum_voters" {
  description = "Existing KRaft controller quorum, for example 100@controller-0...:9093"
  type        = string
}

variable "broker_node_id_base" {
  description = "Node ID added to the StatefulSet ordinal"
  type        = number
  default     = 1
}

variable "demo_external_listener_enabled" {
  description = "Expose per-broker localhost listeners for the isolated Kind demo only"
  type        = bool
  default     = false
}

variable "kafka_heap_opts" {
  description = "Kafka JVM heap settings"
  type        = string
  default     = "-Xms512m -Xmx1g"
}

variable "broker_cpu_request" {
  description = "CPU requested by each broker"
  type        = string
  default     = "500m"
}

variable "broker_memory_request" {
  description = "Memory requested by each broker"
  type        = string
  default     = "1Gi"
}

variable "broker_cpu_limit" {
  description = "CPU limit for each broker"
  type        = string
  default     = "1"
}

variable "broker_memory_limit" {
  description = "Memory limit for each broker"
  type        = string
  default     = "2Gi"
}

variable "storage_class_name" {
  description = "StorageClass used by existing broker PVCs"
  type        = string
}

variable "data_volume_size" {
  description = "Size of each broker data PVC"
  type        = string
  default     = "20Gi"
}
