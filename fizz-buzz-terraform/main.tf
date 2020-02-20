terraform {
  backend "gcs" {
    bucket      = "fizzbuzz-streams-dev"
    prefix      = "/metadata"
    credentials = ".terraform/gcp-service-account-key.json"
  }
}

provider "google-beta" {
  credentials = file(var.gcp_credentials)
  project     = var.gcp_project
  region      = var.gcp_region
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
  kube_cluster        = google_container_cluster.kubezz.name
  available_locations = [data.google_compute_zones.available.names[0]]
}

module "prod" {
  source              = "./environments/prod/"
  gcp_region          = var.gcp_region
  kube_cluster        = google_container_cluster.kubezz.name
  available_locations = data.google_compute_zones.available.names
}