resource "aws_ecr_repository" "football_repository" {
  name = "football"
  # additional configurations...
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}
