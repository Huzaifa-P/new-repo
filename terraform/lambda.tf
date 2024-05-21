# EXTRACTION LAMBDA
resource "null_resource" "install_python_dependencies_for_extraction" {
  provisioner "local-exec" {
    command = "bash ../scripts/extraction-lambda.sh"
  }
}

resource "aws_lambda_function" "extraction_lambda" {
  depends_on    = [null_resource.install_python_dependencies_for_extraction]
  filename      = "../src/extraction/extraction-lambda.zip"
  runtime       = "python3.7"
  function_name = "extraction_lambda"
  # source_code_hash = filebase64sha256("../src/extraction/extraction-lambda.zip")
  role          = aws_iam_role.iam_for_extraction_lambda.arn
  handler       = "extraction.handler"
  layers        = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python37:5", "arn:aws:lambda:eu-west-2:898466741470:layer:psycopg2-py37:1"]
  memory_size   = 1024
  timeout       = 300
}

resource "aws_lambda_permission" "allow_scheduler_for_extraction" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.extraction_lambda.function_name
  principal      = "events.amazonaws.com"
  source_arn     = aws_cloudwatch_event_rule.extraction_lambda_scheduler.arn
  source_account = data.aws_caller_identity.current.account_id
}


# TRANSFORMATION LAMBDA

resource "null_resource" "install_python_dependencies_for_transformation" {
  provisioner "local-exec" {
    command = "bash ../scripts/transformation-lambda.sh"
  }
}

resource "aws_lambda_function" "transformation_lambda" {
  depends_on    = [null_resource.install_python_dependencies_for_transformation]
  filename      = "../src/transformation/transformation-lambda.zip"
  runtime       = "python3.7"
  function_name = "transformation_lambda"
  # source_code_hash = filebase64sha256("../src/transformation/transformation.py")
  role          = aws_iam_role.iam_for_transformation_lambda.arn
  handler       = "transformation.handler"
  layers        = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python37:5"]
  memory_size   = 512
  timeout       = 300
}

resource "aws_lambda_permission" "start_transformation_when_data_enters_ingestion_bucket" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.transformation_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingestion_zone_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}


# LOADING LAMBDA

resource "null_resource" "install_python_dependencies_for_loading" {
  provisioner "local-exec" {
    command = "bash ../scripts/loading-lambda.sh"
  }
}

resource "aws_lambda_function" "loading_lambda" {
  depends_on    = [null_resource.install_python_dependencies_for_loading]
  filename      = "../src/loading/loading-lambda.zip"
  runtime       = "python3.7"
  function_name = "loading_lambda"
  # source_code_hash = filebase64sha256("../src/loading/loading-lambda.zip")
  role          = aws_iam_role.iam_for_loading_lambda.arn
  handler       = "loading.handler"
  layers        = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python37:5", "arn:aws:lambda:eu-west-2:898466741470:layer:psycopg2-py37:1"]
  memory_size   = 512
  timeout       = 600
}

resource "aws_lambda_permission" "start_loading_when_data_enters_processed_bucket" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.loading_lambda.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.processed_data_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}