provider "google-beta" {
  version      = "2.20"
  project      = var.gcp_project
  region       = var.gcp_region
  access_token = data.google_service_account_access_token.default.access_token
}

terraform {
  backend "gcs" {
    bucket = "edml"
    prefix = "/metadata/gitlab"
  }
}

resource "google_container_cluster" "kubezz" {
  name                     = "fizz-buzz-clusterzz"
  project                  = var.gcp_project
  location                 = var.gcp_region
  initial_node_count       = 1
  remove_default_node_pool = true
  node_locations           = data.google_compute_zones.available.names
}

module "staging" {
  source              = "./environments/staging"
  gcp_region          = var.gcp_region
  gcp_project         = var.gcp_project
  kube_cluster        = google_container_cluster.kubezz.name
  available_locations = [data.google_compute_zones.available.names[0]]
}

module "prod" {
  source              = "./environments/prod/"
  gcp_region          = var.gcp_region
  gcp_project         = var.gcp_project
  kube_cluster        = google_container_cluster.kubezz.name
  available_locations = data.google_compute_zones.available.names
}
