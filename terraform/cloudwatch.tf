# EXTRACTION LAMBDA LOGS & ALARM

resource "aws_cloudwatch_log_group" "extraction_lambda" {
  name = "/aws/lambda/extraction_lambda"
}

resource "aws_cloudwatch_log_metric_filter" "extraction_lambda_errors" {
  name           = "ExtractionLambdaErrors"
  pattern        = "Error"
  log_group_name = aws_cloudwatch_log_group.extraction_lambda.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "Error"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "extraction_lambda_alert" {
  alarm_name          = "extraction_lambda_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Error"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_actions       = [aws_sns_topic.lambda_errors.arn]
}


# TRANSFORMATION LAMBDA LOGS & ALARM

resource "aws_cloudwatch_log_group" "transformation_lambda" {
  name = "/aws/lambda/transformation_lambda"
}

resource "aws_cloudwatch_log_metric_filter" "transformation_lambda_errors" {
  name           = "TransformationLambdaErrors"
  pattern        = "Error"
  log_group_name = aws_cloudwatch_log_group.transformation_lambda.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "Error"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "transformation_lambda_alert" {
  alarm_name          = "transformation_lambda_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Error"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_actions       = [aws_sns_topic.lambda_errors.arn]
}

# LOADING LAMBDA LOGS & ALARM

resource "aws_cloudwatch_log_group" "loading_lambda" {
  name = "/aws/lambda/loading_lambda"
}

resource "aws_cloudwatch_log_metric_filter" "loading_lambda_errors" {
  name           = "LoadingLambdaErrors"
  pattern        = "Error"
  log_group_name = aws_cloudwatch_log_group.loading_lambda.name

  metric_transformation {
    name      = "ErrorCount"
    namespace = "Error"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "loading_lambda_alert" {
  alarm_name          = "loading_lambda_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Error"
  period              = 60
  statistic           = "Sum"
  threshold           = 1
  alarm_actions       = [aws_sns_topic.lambda_errors.arn]
}