def distribution:
  map(select(. != null)) | sort |
  if length == 0 then
    {min: null, median: null, max: null}
  else
    . as $values |
    {
      min: $values[0],
      median:
        (if ($values | length) % 2 == 1
         then $values[(($values | length) / 2 | floor)]
         else
           (($values[(($values | length) / 2) - 1] +
             $values[($values | length) / 2]) / 2)
         end),
      max: $values[-1]
    }
  end;

def summarize:
  sort_by([
    .scenario,
    .workload.clients,
    (.async_configuration.batch_size // 0),
    (.async_configuration.max_wait_ms // 0)
  ]) |
  group_by([
    .scenario,
    .workload.clients,
    (.async_configuration.batch_size // 0),
    (.async_configuration.max_wait_ms // 0)
  ]) |
  map(
    . as $trials |
    {
      id:
        (if $trials[0].scenario == "s2-envelope"
         then
           "b\($trials[0].async_configuration.batch_size)-w\($trials[0].async_configuration.max_wait_ms)"
         else $trials[0].scenario
         end),
      scenario: $trials[0].scenario,
      clients: $trials[0].workload.clients,
      async_configuration: $trials[0].async_configuration,
      repetitions: ($trials | length),
      ops_per_sec:
        ($trials | map(.result.ops_per_sec) | distribution),
      end_to_end_ops_per_sec:
        ($trials | map(.result.end_to_end_ops_per_sec) | distribution),
      p99_ms:
        ($trials | map(.result.p99_ms) | distribution),
      max_ms:
        ($trials | map(.result.max_ms) | distribution),
      server_total_core_percent:
        ($trials | map(.server.total_core_percent) | distribution),
      capture_settle_seconds:
        ($trials | map(.workload.capture_settle_seconds) | distribution),
      async_queue_high_water:
        ($trials | map(
          if .async_configuration == null
          then null
          else .module.async_queue_high_water
          end
        ) | distribution),
      achieved_envelope_size:
        ($trials | map(
          if .module.async_envelopes > 0
          then .module.async_envelope_events / .module.async_envelopes
          else null
          end
        ) | distribution),
      envelope_event_percent:
        ($trials | map(
          if .module.async_envelope_events > 0
          then (.module.async_envelope_events / .workload.requests) * 100
          else null
          end
        ) | distribution),
      fallback_percent:
        ($trials | map(
          if .async_configuration == null
          then null
          else (.module.async_fallbacks / .workload.requests) * 100
          end
        ) | distribution),
      correctness: {
        events_lost: ($trials | map(.module.events_lost) | add),
        dropped: ($trials | map(.module.dropped) | add),
        handler_panics: ($trials | map(.module.handler_panics) | add),
        async_worker_errors:
          ($trials | map(.module.async_worker_errors) | add)
      }
    }
  );

def is_healthy:
  (.correctness.events_lost == 0) and
  (.correctness.dropped == 0) and
  (.correctness.handler_panics == 0) and
  (.correctness.async_worker_errors == 0);

def dominates($left; $right):
  ($left.end_to_end_ops_per_sec.median >=
    $right.end_to_end_ops_per_sec.median) and
  ($left.p99_ms.median <= $right.p99_ms.median) and
  ($left.server_total_core_percent.median <=
    $right.server_total_core_percent.median) and
  (
    ($left.end_to_end_ops_per_sec.median >
      $right.end_to_end_ops_per_sec.median) or
    ($left.p99_ms.median < $right.p99_ms.median) or
    ($left.server_total_core_percent.median <
      $right.server_total_core_percent.median)
  );

. as $runs |
[$runs[].trials[]] as $trials |
($trials | summarize) as $summaries |
($summaries | map(
  select(
    is_healthy and
    (.scenario == "s2-sync" or .scenario == "s2-envelope")
  )
)) as $healthy |
{
  schema_version: 1,
  source_runs:
    ($runs | map({
      run_id,
      git_commit,
      started_at,
      completed_at,
      environment,
      capture_configuration
    })),
  trial_count: ($trials | length),
  summaries: $summaries,
  pareto_frontier:
    [
      $healthy[] as $candidate |
      select(
        [
          $healthy[] as $other |
          select(
            ($other.id != $candidate.id) and
            ($other.clients == $candidate.clients) and
            dominates($other; $candidate)
          )
        ] | length == 0
      ) |
      $candidate
    ],
  all_healthy: ($summaries | all(is_healthy))
}
