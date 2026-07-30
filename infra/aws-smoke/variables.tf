variable "aws_region" {
  description = "AWS region for the disposable lab."
  type        = string
  default     = "us-west-2"
}

variable "availability_zone" {
  description = "Optional availability zone. The first available zone is used when empty."
  type        = string
  default     = ""
}

variable "owner" {
  description = "Human owner used for cost attribution and stale-resource cleanup."
  type        = string

  validation {
    condition     = length(trimspace(var.owner)) > 0
    error_message = "owner must be non-empty; set TF_VAR_owner or pass -var owner=..."
  }
}

variable "name_prefix" {
  description = "Name prefix for lab resources."
  type        = string
  default     = "eventstream-smoke"
}

variable "ttl_hours" {
  description = "Advisory expiry tag in hours. Terraform still requires an explicit destroy."
  type        = number
  default     = 4

  validation {
    condition     = var.ttl_hours >= 1 && var.ttl_hours <= 24
    error_message = "ttl_hours must be between 1 and 24."
  }
}

variable "server_instance_type" {
  description = "Non-burstable x86 instance used for Redis."
  type        = string
  default     = "c7i.large"
}

variable "loadgen_instance_type" {
  description = "Non-burstable x86 instance used for redis-benchmark."
  type        = string
  default     = "c7i.large"
}

variable "root_volume_gib" {
  description = "Encrypted gp3 root volume size for each host."
  type        = number
  default     = 16
}

variable "module_image" {
  description = "Pinned multi-arch module image. The default resolves to the v0.4.0 manifest."
  type        = string
  default     = "ghcr.io/joshrotenberg/redis-event-stream-module:0.4.0@sha256:1466a273fd64321ca3eba4447db3a16e2eb231be2afdb3a620c5cb2229be4db2"
}

variable "loadgen_image" {
  description = "Pinned Redis image supplying redis-cli and redis-benchmark."
  type        = string
  default     = "redis:8.8.0@sha256:234c902a2db49461a129e2d4aeff85b28cf20187ed274a67f6e50995fa713c7b"
}

variable "extra_tags" {
  description = "Additional tags applied to every taggable resource."
  type        = map(string)
  default     = {}
}
