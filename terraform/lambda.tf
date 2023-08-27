# Loader from football-data.co.uk
resource "aws_lambda_function" "football_data_co_uk_loader" {
  function_name = "football_data_co_uk_loader"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:aws_ingestion_lambda_tasks"
  package_type  = "Image"
  image_config {
    command = ["football_data_co_uk.scrape_results_handler"]
  }
  role    = aws_iam_role.lambda_ex.arn
  timeout = "900"
}

resource "aws_lambda_function" "clean_football_data_co_uk" {
  function_name = "clean_football_data_co_uk"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:aws_ingestion_lambda_tasks"
  package_type = "Image"
  image_config {
    command = ["football_data_co_uk.clean_footballdata_handler"]
  }
  role    = aws_iam_role.lambda_ex.arn
}

# Pandas-based util that appended all partition csvs into single dataframe, which resolves column alignment issues
resource "aws_lambda_function" "consolidate_footballdata_lambda" {
  function_name = "consolidate_footballdata_lambda"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:aws_ingestion_lambda_tasks"
  role          = aws_iam_role.team-lineups-consumer-role.arn
  package_type  = "Image"
  image_config {
    command = ["football_data_co_uk.consolidate_footballdata_handler"]
  }
  timeout = 60
}

# Make sure that the raw xg result files have same columns
resource "aws_lambda_function" "standardise-raw-xg-csvs" {
  function_name = "standardise-raw-xg-csvs"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:aws_ingestion_lambda_tasks"
  role          = aws_iam_role.lambda_ex.arn
  package_type  = "Image"
  image_config {
    command = ["fb_ref.standardise_current_xg_results_files_handler"]
  }
  timeout = 60
}

# Load the lineups objects from the team lineups queue
resource "aws_lambda_function" "team_lineups_loader" {
  depends_on    = [aws_iam_role.team-lineups-consumer-role]
  function_name = "team_lineups_loader"
  image_uri     = "${aws_ecr_repository.football_repository.repository_url}:aws_ingestion_lambda_tasks"
  package_type  = "Image"
  image_config {
    command = ["fb_ref.team_lineups_loader_handler"]
  }
  role    = aws_iam_role.team-lineups-consumer-role.arn
  timeout = "30"

}

resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.football_data_co_uk_raw.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.clean_football_data_co_uk.arn
    events              = ["s3:ObjectCreated:*"]  # Triggered when an object is created. Adjust event types as necessary.
    filter_suffix = ".csv"
    # You can also specify filters like prefix/suffix if needed:
    # filter_prefix       = "logs/"
    # filter_suffix       = ".log"
  }
}


