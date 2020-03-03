resource "google_container_node_pool" "staging" {
  provider   = google-beta
  project    = var.gcp_project
  name       = "staging-node-poolzz"
  location   = var.gcp_region
  cluster    = var.kube_cluster
  node_count = 1

  depends_on = [
    var.kube_cluster
  ]

  node_locations = [
    "${var.gcp_region}-b",
  ]

  node_config {
    preemptible  = true
    machine_type = "n1-standard-1"

    labels = {
      env = "staging"
    }

    tags = ["staging", "kafka-clients"]
  }
}