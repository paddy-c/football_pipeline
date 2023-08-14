resource "aws_lambda_function" "football_data_co_uk_loader" {
  filename      = "../payload.zip"
  function_name = "football_data_co_uk_loader"
  handler       = "football_data_co_uk.scrape_results_handler"
  role          = aws_iam_role.lambda_ex.arn
  runtime       = "python3.9"
  timeout       = "900"
}


resource "aws_lambda_function" "consolidate_footballdata_lambda" {
  function_name = "consolidate_footballdata_lambda"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:new_consolidate_footballdata"
  role          = aws_iam_role.team-lineups-consumer-role.arn
  package_type  = "Image"
  timeout       = 60
}


resource "aws_lambda_function" "standardise-raw-xg-csvs" {
  function_name = "standardise-raw-xg-csvs"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:standardise_raw_xg_csvs_v5"
  role          = aws_iam_role.lambda_ex.arn
  package_type  = "Image"
  timeout       = 60
}

