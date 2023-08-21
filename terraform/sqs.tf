resource "aws_sqs_queue" "team-lineups-queue" {
  name                       = "team-lineups-queue"
  fifo_queue                 = false
  delay_seconds              = 0
  visibility_timeout_seconds = 300
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.team-lineups-dead-letter-queue.arn
    maxReceiveCount     = 4
  })
}

resource "aws_lambda_event_source_mapping" "sqs_event_source" {
  event_source_arn = aws_sqs_queue.team-lineups-queue.arn
  function_name    = aws_lambda_function.team_lineups_loader.function_name
  enabled          = true
  batch_size       = 10
}

resource "aws_sqs_queue" "team-lineups-dead-letter-queue" {
  name                       = "team-lineups-dead-letter-queue"
  fifo_queue                 = false
  delay_seconds              = 0
  visibility_timeout_seconds = 30
}