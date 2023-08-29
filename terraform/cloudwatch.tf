resource "aws_cloudwatch_event_rule" "daily" {
  name                = "trigger-lambda-daily"
  description         = "Trigger my lambda every day"
  schedule_expression = "cron(17 8 * * ? *)" # This cron expression triggers at 12:00 PM (UTC) every day
}

resource "aws_cloudwatch_event_target" "trigger_daily_update_of_football_data_co_uk" {
  rule      = aws_cloudwatch_event_rule.daily.name
  target_id = "football_data_co_uk_loader"
  arn       = aws_lambda_function.football_data_co_uk_loader.arn

  input = jsonencode({
    mode = "update",
    # Add any other payload keys/values you want to send to the lambda
  })
}

resource "aws_cloudwatch_event_target" "trigger_daily_update_of_fbref" {
  rule      = aws_cloudwatch_event_rule.daily.name
  target_id = "xg_results_loader"
  arn       = aws_lambda_function.load-raw-xg-csvs.arn
  input = jsonencode({
    # Add any other payload keys/values you want to send to the lambda
  })
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.football_data_co_uk_loader.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}

resource "aws_lambda_permission" "fbref_loader_allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load-raw-xg-csvs.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}