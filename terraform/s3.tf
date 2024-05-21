resource "aws_s3_bucket" "ingestion_zone_bucket" {
  bucket_prefix = "nc-project-ingestion-zone-"
  force_destroy = true
}

resource "aws_s3_bucket" "processed_data_bucket" {
  bucket_prefix = "nc-project-processed-data-"
  force_destroy = true
}

resource "aws_s3_bucket_notification" "ingestion_zone_lambda_trigger" {
  bucket = aws_s3_bucket.ingestion_zone_bucket.bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.transformation_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

}

resource "aws_s3_bucket_notification" "processed_zone_lambda_trigger" {
  bucket = aws_s3_bucket.processed_data_bucket.bucket

  lambda_function {
    lambda_function_arn = aws_lambda_function.loading_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }

}