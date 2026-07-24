# This deliberately empty module lets Terraform test clean mock state after
# exercising resources protected by lifecycle.prevent_destroy.

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.30, < 7.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}
