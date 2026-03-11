
# Data Lake (GCS Bucket)
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${var.gcs_bucket_name}_${var.project_id}"
  location      = var.region
  force_destroy = true

  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 60  # Keep files for 30 days (Senior tip: saves money in dev!)
    }
  }
}

# Data Warehouse (BigQuery Dataset)
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project_id
  location   = var.region
}
