terraform {
  backend "gcs" {
    bucket = "fizzbuzz-streams-dev"
    prefix = "/metadata"
    credentials = ".terraform/gcp-service-account-key.json"
  }
}

provider "google" {
  credentials = file(var.gcp_credentials)
  project = var.gcp_project
  region = var.gcp_region
}