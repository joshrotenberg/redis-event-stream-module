def classified:
  . + {
    healthy:
      ((.target_achievement_ratio.median // 0) >= $achievement_ratio and
       .p99_ms.median != null and .p99_ms.median <= $p99_budget_ms)
  };

[.[] | select(.target_ops_per_sec != null)] |
sort_by([
  .scenario,
  (.selectivity_percent // -1),
  .clients_per_thread,
  .threads,
  .pipeline,
  .target_ops_per_sec
]) |
group_by([
  .scenario,
  (.selectivity_percent // -1),
  .clients_per_thread,
  .threads,
  .pipeline
]) |
map(
  map(classified) as $levels |
  ([$levels[] | select(.healthy)] |
    if length == 0 then null else max_by(.target_ops_per_sec) end) as $at |
  (if $at == null then []
   else [$levels[] |
     select(.healthy == false and .target_ops_per_sec < $at.target_ops_per_sec)]
   end) as $non_monotonic |
  {
    scenario: $levels[0].scenario,
    selectivity_percent: $levels[0].selectivity_percent,
    clients_per_thread: $levels[0].clients_per_thread,
    threads: $levels[0].threads,
    pipeline: $levels[0].pipeline,
    criterion: {
      p99_budget_ms: $p99_budget_ms,
      minimum_target_achievement_ratio: $achievement_ratio
    },
    status:
      (if ($non_monotonic | length) > 0 then "non-monotonic"
       elif $at == null then "no-healthy-point"
       elif ([$levels[] |
         select(.target_ops_per_sec > $at.target_ops_per_sec)] | length) == 0
         then "ceiling-not-reached"
       else "bracketed"
       end),
    below:
      (if $at == null then null
       else ([$levels[] |
         select(.target_ops_per_sec < $at.target_ops_per_sec)] |
         if length == 0 then null else max_by(.target_ops_per_sec) end)
       end),
    at: $at,
    above:
      (if $at == null then ($levels | min_by(.target_ops_per_sec))
       else ([$levels[] |
         select(.target_ops_per_sec > $at.target_ops_per_sec)] |
         if length == 0 then null else min_by(.target_ops_per_sec) end)
       end),
    non_monotonic_levels: $non_monotonic,
    all_levels: $levels
  }
)
