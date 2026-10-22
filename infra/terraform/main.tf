data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "lake" {
  bucket_prefix = "clickstream-cohorts-"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_glue_catalog_database" "project" {
  for_each = toset(["rees46", "staging", "intermediate", "core", "marts"])
  name     = each.value
}

resource "aws_glue_catalog_table" "events_silver" {
  name          = "events_silver"
  database_name = aws_glue_catalog_database.project["rees46"].name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    classification        = "parquet"
    "projection.enabled"  = "true"
    "projection.event_date.type"   = "date"
    "projection.event_date.format" = "yyyy-MM-dd"
    "projection.event_date.range"  = "2019-10-01,2019-11-30"
    "storage.location.template"    = "s3://${aws_s3_bucket.lake.id}/silver/events/event_date=$${event_date}/"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/silver/events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }
    columns {
      name = "event_time"
      type = "timestamp"
    }
    columns {
      name = "event_type"
      type = "string"
    }
    columns {
      name = "product_id"
      type = "bigint"
    }
    columns {
      name = "category_id"
      type = "bigint"
    }
    columns {
      name = "category_code"
      type = "string"
    }
    columns {
      name = "brand"
      type = "string"
    }
    columns {
      name = "price"
      type = "double"
    }
    columns {
      name = "user_id"
      type = "bigint"
    }
    columns {
      name = "user_session"
      type = "string"
    }
  }

  partition_keys {
    name = "event_date"
    type = "date"
  }
}

resource "aws_athena_workgroup" "pipeline" {
  name = "clickstream-to-cohorts"

  configuration {
    bytes_scanned_cutoff_per_query      = var.bytes_scanned_cutoff_per_query
    enforce_workgroup_configuration     = true
    publish_cloudwatch_metrics_enabled  = false

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

locals {
  rendered_policy = replace(
    replace(
      replace(
        file("${path.module}/../iam_policy.json"),
        "BUCKET_NAME",
        aws_s3_bucket.lake.id
      ),
      "ACCOUNT_ID",
      data.aws_caller_identity.current.account_id
    ),
    "REGION",
    var.aws_region
  )
}

resource "aws_iam_policy" "pipeline" {
  name        = "clickstream-to-cohorts"
  description = "One bucket, the project Glue databases, and the project Athena workgroup."
  policy      = local.rendered_policy
}

resource "aws_budgets_budget" "monthly" {
  count        = var.budget_alert_email == "" ? 0 : 1
  name         = "clickstream-to-cohorts-monthly"
  budget_type  = "COST"
  limit_amount = "5"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  time_period_start = "2026-10-01_00:00"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
