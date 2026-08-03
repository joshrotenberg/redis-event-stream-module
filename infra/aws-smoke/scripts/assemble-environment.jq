{
  schema_version: ($schema_version | tonumber),
  collected_at: $collected_at,
  region: $region,
  availability_zone: $availability_zone,
  network: {
    vpc_id: $vpc_id,
    subnet_id: $subnet_id,
    server_private_ip: $server_private_ip
  },
  storage_contract: {
    root_volume_type: $root_volume_type,
    root_volume_gib: $root_volume_gib,
    encrypted: true,
    delete_on_termination: true
  },
  hard_stop: {
    expires_at: $expires_at,
    scheduler_arn: $expiry_stop_schedule_arn
  },
  topology: {
    kind: "standalone",
    server_nodes: 1,
    load_generator_nodes: 1
  },
  hosts: {
    server: $server_host[0],
    load_generator: $loadgen_host[0]
  },
  instances:
    [$instances[0].Reservations[].Instances[] | {
      role:
        (if .InstanceId == $server_id then "server"
         elif .InstanceId == $loadgen_id then "load_generator"
         else "unknown"
         end),
      instance_id: .InstanceId,
      instance_type: .InstanceType,
      image_id: .ImageId,
      architecture: .Architecture,
      private_ip: .PrivateIpAddress,
      vpc_id: .VpcId,
      subnet_id: .SubnetId,
      availability_zone: .Placement.AvailabilityZone,
      cpu_options: .CpuOptions
    }],
  volumes:
    [$volumes[0].Volumes[] | {
      volume_id: .VolumeId,
      instance_id: .Attachments[0].InstanceId,
      device: .Attachments[0].Device,
      type: .VolumeType,
      size_gib: .Size,
      iops: .Iops,
      throughput_mib_s: .Throughput,
      encrypted: .Encrypted
    }]
}
