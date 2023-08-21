resource "aws_iam_role" "lambda_ex" {
  assume_role_policy = jsonencode({
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
    Version = "2012-10-17"
  })

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
  ]
}

#resource "aws_iam_policy" "lambda_sqs_full" {
#  name        = "lambdasqsreceivemessagepolicy"
#  description = "allows lambda to call receivemessage on sqs"
#
#  policy = jsonencode({
#    version = "2012-10-17",
#    statement = [{
#      action    = "sqs:*",
#      effect    = "Allow",
#      resource  = aws_sqs_queue.team-lineups-queue.arn
#    }]
#  })
#}

resource "aws_iam_role" "team-lineups-consumer-role" {
  #depends_on = [aws_iam_policy.lambda_sqs_full]
  assume_role_policy = jsonencode(
    {
      Statement = [
        {
          Action = "sts:AssumeRole"
          Effect = "Allow"
          Principal = {
            Service = "lambda.amazonaws.com"
          }
        }
      ]
      Version = "2012-10-17"
    }
  )

  managed_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
    "arn:aws:iam::aws:policy/AmazonSQSFullAccess" # try adding sqs full access here
  ]
}

#resource "aws_iam_role_policy_attachment" "lambda_sqs_full_attach" {
#  policy_arn = aws_iam_policy.lambda_sqs_full.arn
#  role       = aws_iam_role.team-lineups-consumer-role.name
#  depends_on = [aws_iam_policy.lambda_sqs_full, aws_iam_role.team-lineups-consumer-role]
#}
