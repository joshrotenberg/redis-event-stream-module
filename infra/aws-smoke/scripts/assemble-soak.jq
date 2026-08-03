def telemetry_analysis($samples):
  if ($samples | length) < 2 then null
  else
    ($samples[0]) as $first |
    ($samples[-1]) as $last |
    {
      samples: ($samples | length),
      elapsed_seconds: (($last.epoch_ms - $first.epoch_ms) / 1000),
      redis_main_core_percent:
        ((($last.used_cpu_main_seconds - $first.used_cpu_main_seconds) /
          (($last.epoch_ms - $first.epoch_ms) / 1000)) * 100),
      redis_total_core_percent:
        ((($last.used_cpu_total_seconds - $first.used_cpu_total_seconds) /
          (($last.epoch_ms - $first.epoch_ms) / 1000)) * 100),
      max_queue_depth: ($samples | map(.queue_depth) | max),
      final_queue_depth: $last.queue_depth,
      max_consumer_lag: ($samples | map(.consumer_lag) | max),
      final_consumer_lag: $last.consumer_lag,
      rss_bytes: {
        first: $first.used_memory_rss_bytes,
        max: ($samples | map(.used_memory_rss_bytes) | max),
        final: $last.used_memory_rss_bytes
      },
      stream_memory_bytes: {
        first: $first.stream_memory_bytes,
        max: ($samples | map(.stream_memory_bytes) | max),
        final: $last.stream_memory_bytes
      }
    }
  end;

($graceful_probe[0].durable_source_keys_after_boundary -
 $graceful_audit[0].logical_events) as $graceful_missing |
($abrupt_probe[0].durable_source_keys_after_boundary -
 $abrupt_audit[0].logical_events) as $abrupt_missing |
{
  schema_version: ($schema_version | tonumber),
  run_id: $run_id,
  started_at: $started_at,
  completed_at: $completed_at,
  git_commit: $git_commit,
  environment: {
    region: $region,
    availability_zone: $availability_zone,
    server_instance_type: $server_instance_type,
    loadgen_instance_type: $loadgen_instance_type,
    module_image: $module_image,
    module_build_profile: "release",
    module_git_commit: $module_artifact.git_commit,
    module_artifact: $module_artifact,
    consumer_artifact: $client_artifact,
    loadgen_image: $loadgen_image,
    expires_at: $expires_at,
    lab: $lab_environment[0]
  },
  plan: $plan[0],
  calibration: $calibration[0],
  main: ($main[0] + {
    analysis: telemetry_analysis($main[0].telemetry)
  }),
  graceful: {
    probe: $graceful_probe[0],
    audit: $graceful_audit[0],
    missing_logical_events: $graceful_missing,
    exact:
      ($graceful_probe[0].threshold_reached and
       ($graceful_probe[0].queue_depth_before_boundary > 0) and
       ($graceful_missing == 0))
  },
  abrupt: {
    probe: $abrupt_probe[0],
    audit: $abrupt_audit[0],
    acknowledged_not_durable:
      ($abrupt_probe[0].acknowledged_commands -
       $abrupt_probe[0].durable_source_keys_after_boundary),
    missing_logical_events: $abrupt_missing,
    missing_vs_observed_queue:
      (if $abrupt_probe[0].queue_depth_before_boundary > 0
       then
         ($abrupt_missing /
          $abrupt_probe[0].queue_depth_before_boundary)
       else null
       end),
    accounting_valid:
      ($abrupt_probe[0].threshold_reached and
       ($abrupt_probe[0].queue_depth_before_boundary > 0) and
       ($abrupt_missing >= 0))
  }
}
