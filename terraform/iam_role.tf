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


resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.clean_football_data_co_uk.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::football-data-co-uk-raw"
}

# Attach this policy to your Lambda's execution role to allow it to read objects from the S3 bucket
data "aws_iam_policy_document" "lambda_read_access" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::football-data-co-uk-raw/*"]
    effect    = "Allow"
  }
}

# You'd then create an aws_iam_policy and attach it to the Lambda's execution role.
resource "aws_iam_policy" "lambda_s3_read_policy" {
  name        = "LambdaS3ReadAccess"
  description = "Allows lambda to read from S3 bucket"
  policy      = data.aws_iam_policy_document.lambda_read_access.json
}

resource "aws_iam_role_policy_attachment" "attach_lambda_s3_read_policy" {
  policy_arn = aws_iam_policy.lambda_s3_read_policy.arn
  role       = aws_iam_role.lambda_ex.name
}
