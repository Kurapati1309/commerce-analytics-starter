terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket
}

resource "aws_s3_bucket" "processed" {
  bucket = var.processed_bucket
}

resource "aws_s3_bucket" "dbt_artifacts" {
  bucket = var.dbt_artifacts_bucket
}

# (Optional) Redshift Serverless and IAM roles would go here.
