resource "aws_cloudwatch_event_rule" "daily_11pm" {
  name                = "trigger-lambda-daily-11pm"
  description         = "Trigger my lambda at 11pm every day"
  schedule_expression = "cron(0 23 * * ? *)"
}

resource "aws_cloudwatch_event_rule" "daily" {
  name                = "trigger-lambda-daily"
  description         = "Trigger my lambda every day"
  schedule_expression = "cron(17 8 * * ? *)"
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

resource "aws_cloudwatch_event_target" "trigger_daily_update_of_fbref_goal_logs" {
  rule      = aws_cloudwatch_event_rule.daily_11pm.name
  target_id = "extract-and-load-current-season-goal-logs"
  arn       = aws_lambda_function.extract-and-load-current-season-goal-logs.arn
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

resource "aws_lambda_permission" "goal_logs_loader_allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.extract-and-load-current-season-goal-logs.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_11pm.arn
}