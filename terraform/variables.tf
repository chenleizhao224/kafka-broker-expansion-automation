variable "kube_context" {
  description = "kubectl context containing the existing Kafka cluster"
  type        = string
  default     = null
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

variable "broker_replicas" {
  description = "Desired broker count. The Python guard only permits changing 2 to 3."
  type        = number
  default     = 2

  validation {
    condition     = contains([2, 3], var.broker_replicas)
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
  default     = 0
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

