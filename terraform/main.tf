provider "kubernetes" {
  config_path    = pathexpand(var.kube_config_path)
  config_context = var.kube_context
}

locals {
  labels = {
    "app.kubernetes.io/name"       = "kafka"
    "app.kubernetes.io/component"  = "broker"
    "app.kubernetes.io/managed-by" = "terraform"
  }
}

resource "kubernetes_service_v1" "kafka_headless" {
  metadata {
    name      = var.statefulset_name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    cluster_ip                  = "None"
    publish_not_ready_addresses = true
    selector                    = local.labels

    port {
      name        = "broker"
      port        = 9092
      target_port = "broker"
    }
  }
}

resource "kubernetes_service_v1" "kafka_bootstrap" {
  metadata {
    name      = "${var.statefulset_name}-bootstrap"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    selector = local.labels

    port {
      name        = "broker"
      port        = 9092
      target_port = "broker"
    }
  }
}

resource "kubernetes_stateful_set_v1" "kafka" {
  metadata {
    name      = var.statefulset_name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    replicas              = var.broker_count
    service_name          = kubernetes_service_v1.kafka_headless.metadata[0].name
    pod_management_policy = "OrderedReady"

    selector {
      match_labels = local.labels
    }

    template {
      metadata {
        labels = local.labels
      }

      spec {
        termination_grace_period_seconds = 60

        security_context {
          fs_group = 1000
        }

        container {
          name  = "kafka"
          image = var.kafka_image

          command = ["/bin/bash", "-ec"]
          args = [<<-EOT
            ordinal="$${HOSTNAME##*-}"
            export KAFKA_NODE_ID="$(( ${var.broker_node_id_base} + ordinal ))"
            if [[ "${var.demo_external_listener_enabled}" == "true" ]]; then
              external_port="$((19092 + ordinal))"
              export KAFKA_ADVERTISED_LISTENERS="INTERNAL://$${HOSTNAME}.${var.statefulset_name}.${var.namespace}.svc.cluster.local:9092,EXTERNAL://localhost:$${external_port}"
            else
              export KAFKA_ADVERTISED_LISTENERS="BROKER://$${HOSTNAME}.${var.statefulset_name}.${var.namespace}.svc.cluster.local:9092"
            fi
            exec /etc/kafka/docker/run
          EOT
          ]

          env {
            name  = "CLUSTER_ID"
            value = var.cluster_id
          }
          env {
            name  = "KAFKA_HEAP_OPTS"
            value = var.kafka_heap_opts
          }
          env {
            name  = "KAFKA_PROCESS_ROLES"
            value = "broker"
          }
          env {
            name  = "KAFKA_CONTROLLER_QUORUM_VOTERS"
            value = var.controller_quorum_voters
          }
          env {
            name  = "KAFKA_LISTENERS"
            value = var.demo_external_listener_enabled ? "INTERNAL://:9092,EXTERNAL://:9094" : "BROKER://:9092"
          }
          env {
            name  = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP"
            value = var.demo_external_listener_enabled ? "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT" : "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT"
          }
          env {
            name  = "KAFKA_INTER_BROKER_LISTENER_NAME"
            value = var.demo_external_listener_enabled ? "INTERNAL" : "BROKER"
          }
          env {
            name  = "KAFKA_CONTROLLER_LISTENER_NAMES"
            value = "CONTROLLER"
          }
          env {
            name  = "KAFKA_LOG_DIRS"
            value = "/var/lib/kafka/data"
          }

          port {
            name           = "broker"
            container_port = 9092
          }

          port {
            name           = "external"
            container_port = 9094
          }

          readiness_probe {
            tcp_socket {
              port = "broker"
            }
            initial_delay_seconds = 15
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 12
          }

          resources {
            requests = {
              cpu    = var.broker_cpu_request
              memory = var.broker_memory_request
            }
            limits = {
              cpu    = var.broker_cpu_limit
              memory = var.broker_memory_limit
            }
          }

          security_context {
            allow_privilege_escalation = false
            run_as_non_root            = true
            run_as_user                = 1000
          }

          volume_mount {
            name       = "data"
            mount_path = "/var/lib/kafka/data"
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
      }

      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = var.storage_class_name

        resources {
          requests = {
            storage = var.data_volume_size
          }
        }
      }
    }
  }
}
