variable "aws_region" {
  description = "AWS region for the bucket, Glue catalog, and Athena workgroup."
  type        = string
  default     = "us-east-1"
}

variable "bytes_scanned_cutoff_per_query" {
  description = "Athena per query scan cap in bytes. 10 GiB is about $0.05 at $5 per TB."
  type        = number
  default     = 10737418240
}

variable "budget_alert_email" {
  description = "Email for the monthly 5 USD budget alert. Leave empty to skip the budget."
  type        = string
  default     = ""
}
