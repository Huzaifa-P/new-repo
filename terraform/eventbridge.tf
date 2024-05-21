# EVENTBRIDGE SCHEDULER TO INVOKE EXTRACTION_LAMBDA EVERY 10 MINUTES

resource "aws_cloudwatch_event_rule" "extraction_lambda_scheduler" {
  name_prefix         = "extraction_lambda"
  schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "sns" {
  rule = aws_cloudwatch_event_rule.extraction_lambda_scheduler.name
  arn  = aws_lambda_function.extraction_lambda.arn
}