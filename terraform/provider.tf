provider "aws" {
  region = "eu-west-2"
}

terraform {
  backend "s3" {
    bucket = "nc-project-tf-state"
    key    = "terraform.tfstate"
    region = "eu-west-2"
  }
}