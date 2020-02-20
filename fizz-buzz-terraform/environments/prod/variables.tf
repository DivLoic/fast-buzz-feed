variable "gcp_region" {}

variable "kube_cluster" {}

variable "available_locations" {
  type = list(string)
}