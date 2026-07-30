output "ami_id" {
  description = "Amazon Linux 2023 AMI resolved for this run."
  value       = nonsensitive(data.aws_ssm_parameter.amazon_linux_2023.value)
}

output "availability_zone" {
  value = local.availability_zone
}

output "expires_at" {
  value = time_offset.expiry.rfc3339
}

output "loadgen_instance_id" {
  value = aws_instance.loadgen.id
}

output "loadgen_instance_type" {
  value = aws_instance.loadgen.instance_type
}

output "loadgen_private_ip" {
  value = aws_instance.loadgen.private_ip
}

output "loadgen_image" {
  value = var.loadgen_image
}

output "module_image" {
  value = var.module_image
}

output "region" {
  value = var.aws_region
}

output "server_instance_id" {
  value = aws_instance.server.id
}

output "server_instance_type" {
  value = aws_instance.server.instance_type
}

output "server_private_ip" {
  value = aws_instance.server.private_ip
}
