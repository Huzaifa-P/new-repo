# I AM ROLE FOR LAMDA FUNCTIONS

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "iam_for_extraction_lambda" {
  name               = "iam_for_extraction_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_role" "iam_for_transformation_lambda" {
  name               = "iam_for_transformation_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}

resource "aws_iam_role" "iam_for_loading_lambda" {
  name               = "iam_for_loading_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
}


# I AM POLICY TO WRITE TO CLOUDWATCH LOGS

data "aws_iam_policy_document" "lambda_logging" {

  statement {
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
      "s3:ListBucket",
      "s3:ListAllMyBuckets",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*", "arn:aws:s3:::*", "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

}

# I AM POLICIES FOR EXTRACTION LAMBDA TO WRITE TO CLOUDWATCH LOGS

resource "aws_iam_policy" "extraction_lambda_logging" {
  name        = "extraction_lambda_logging"
  path        = "/"
  description = "IAM policy for logging from extraction lambda"
  policy      = data.aws_iam_policy_document.lambda_logging.json
}

resource "aws_iam_role_policy_attachment" "extraction_lambda_logs" {
  role       = aws_iam_role.iam_for_extraction_lambda.name
  policy_arn = aws_iam_policy.extraction_lambda_logging.arn
}


# I AM POLICIES FOR TRANSFORMATION LAMBDA TO WRITE TO CLOUDWATCH LOGS

resource "aws_iam_policy" "transformation_lambda_logging" {
  name        = "transformation_lambda_logging"
  path        = "/"
  description = "IAM policy for logging from transformation lambda"
  policy      = data.aws_iam_policy_document.lambda_logging.json
}

resource "aws_iam_role_policy_attachment" "transformation_lambda_logs" {
  role       = aws_iam_role.iam_for_transformation_lambda.name
  policy_arn = aws_iam_policy.transformation_lambda_logging.arn
}


# I AM POLICIES FOR LOADING LAMBDA TO WRITE TO CLOUDWATCH LOGS

resource "aws_iam_policy" "loading_lambda_logging" {
  name        = "loading_lambda_logging"
  path        = "/"
  description = "IAM policy for logging from loading lambda"
  policy      = data.aws_iam_policy_document.lambda_logging.json
}

resource "aws_iam_role_policy_attachment" "loading_lambda_logs" {
  role       = aws_iam_role.iam_for_loading_lambda.name
  policy_arn = aws_iam_policy.loading_lambda_logging.arn
}