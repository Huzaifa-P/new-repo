resource "aws_sns_topic" "lambda_errors" {
  name = "lamda-errors"
}

resource "aws_sns_topic_subscription" "lambde_errors_email_target" {
  topic_arn = aws_sns_topic.lambda_errors.arn
  protocol  = "email"
  endpoint  = "hasan.sattar.de-202307@northcoders.net"
}

resource "aws_sns_topic_subscription" "lambde_errors_email_target_2" {
  topic_arn = aws_sns_topic.lambda_errors.arn
  protocol  = "email"
  endpoint  = "kabilan.thayaparan.de-202307@northcoders.net"
}

resource "aws_sns_topic_subscription" "lambde_errors_email_target_3" {
  topic_arn = aws_sns_topic.lambda_errors.arn
  protocol  = "email"
  endpoint  = "chaunjiao.zong.de-202307@northcoders.net"
}

resource "aws_sns_topic_subscription" "lambde_errors_email_target_4" {
  topic_arn = aws_sns_topic.lambda_errors.arn
  protocol  = "email"
  endpoint  = "huzaifa.patel.de-202307@northcoders.net"
}

resource "aws_sns_topic_subscription" "lambde_errors_email_target_5" {
  topic_arn = aws_sns_topic.lambda_errors.arn
  protocol  = "email"
  endpoint  = "caspar.wilson.de-202307@northcoders.net"
}