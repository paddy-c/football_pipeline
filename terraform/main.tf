resource "aws_iam_user_policy" "lambda_layer_access" {
  name = "lambda_layer_access"
  user = "padraig-codecommit"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = "lambda:GetLayerVersion",
        Resource = "*"
      },
    ]
  })
}


resource "aws_instance" "fbref-scraper-server" {
  ami           = "ami-0f34c5ae932e6f0e4"
  instance_type = "t2.micro"

}
