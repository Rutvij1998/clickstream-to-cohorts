output "bucket_name" {
  value = aws_s3_bucket.lake.id
}

output "athena_workgroup" {
  value = aws_athena_workgroup.pipeline.name
}

output "glue_database" {
  value = aws_glue_catalog_database.project["rees46"].name
}

output "athena_results_uri" {
  value = "s3://${aws_s3_bucket.lake.id}/athena-results/"
}

output "dbt_data_uri" {
  value = "s3://${aws_s3_bucket.lake.id}/dbt/"
}

output "iam_policy_arn" {
  value = aws_iam_policy.pipeline.arn
}
