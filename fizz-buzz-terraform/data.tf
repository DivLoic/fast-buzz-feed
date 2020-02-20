data "google_compute_zones" "available" {
  project = var.gcp_project
  region  = var.gcp_region
}