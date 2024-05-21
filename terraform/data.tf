data "aws_caller_identity" "current" {}

data "aws_region" "current" {
  name = "eu-west-2"
}