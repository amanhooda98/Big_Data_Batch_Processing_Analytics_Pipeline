terraform {
  required_version = ">=1.0"
  backend "local" {}
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}


resource "google_compute_firewall" "port_rules" {
  project     = var.project
  name        = "port"
  network     = var.network
  description = "Opens port 8888,4040 in the spark,airflow VM for Spark cluster to connect"

  allow {
    protocol = "tcp"
    ports    = ["8888","4040","8080","80"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["spark","airflow"]

}

resource "google_compute_instance" "spark_vm_instance" {
  name                      = "spark-instance"
  machine_type              = "e2-standard-4"
  tags                      = ["spark"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.vm_image
      size  = 30
    }
  }

  network_interface {
    network = var.network
    access_config {
    }
  }
}


resource "google_compute_instance" "airflow_vm_instance" {
  name                      = "airflow-instance"
  machine_type              = "e2-standard-2"
  tags                      = ["airflow"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = var.vm_image
      size  = 30
    }
  }

  network_interface {
    network = var.network
    access_config {
    }
  }
}

resource "google_storage_bucket" "amanhd9-bucket" {
  name          = var.bucket
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }
}



resource "google_bigquery_dataset" "stg_dataset" {
  dataset_id                 = var.stg_bq_dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id                 = var.prod_bq_dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}
