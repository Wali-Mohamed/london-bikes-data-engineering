variable "credentials" {
  description = "Path to your GCP Service Account JSON key"
  default     = "../google_credentials.json"
}

variable "project_id" {
  description = "Your GCP Project ID"
  default     = "santander-bikes-pipeline"
}

variable "region" {
  description = "Region for GCP resources"
  default     = "europe-west2"
}

variable "storage_class" {
  description = "Storage class type for your bucket"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Target GCS Bucket Name"
  default     = "london_bikes_data_lake"
}

variable "bq_dataset_name" {
  description = "Target BigQuery Dataset Name"
  default     = "london_bikes_dw"
}

