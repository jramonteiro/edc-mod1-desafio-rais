provider "aws" {
  region = var.aws_region
  version = "~> 3.7"

}


terraform {

  backend "s3" {
      bucket = "terraform-state-igti-jeff"
      key = "state/igti/edc/desafio-mod1/terraform.tfstate"
      region = "us-east-2"
  }
}