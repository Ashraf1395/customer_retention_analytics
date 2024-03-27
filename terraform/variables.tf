variable "location" {
  description = "My location"
  default = "US"
}

variable "gcs_storage_class" {
  description = "GCS storage class name"
  default = "raw_streaming"

}

variable "gcs_bucket_name" {
  description = "GCS storage bucket name"
  default = "customer-activity-data-terraform"
}

