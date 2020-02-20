resource "google_container_node_pool" "production" {
  provider = google-beta
  name       = "prod-node-poolzz"
  location   = var.gcp_region
  cluster    = var.kube_cluster
  node_count = 1

  depends_on = [
    var.kube_cluster
  ]

  node_locations = var.available_locations

  node_config {
    preemptible  = true
    machine_type = "n1-standard-2"

    labels = {
      env = "prod"
    }

    tags = ["prod", "kafka-clients"]
  }
}