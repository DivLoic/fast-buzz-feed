terraform {
  backend "gcs" {
    bucket = "edml"
    prefix = "/metadata"
    credentials = ".terraform/edml-gcp-access-key.json"
  }
}

provider "google" {
  credentials = file(var.gcp_credentials)
  project = var.gcp_project
  region = var.gcp_region
}