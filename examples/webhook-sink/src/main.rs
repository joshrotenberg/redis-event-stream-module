use std::collections::BTreeSet;
use std::error::Error;
use std::io;
use std::time::{Duration, Instant};

use clap::Parser;
use eventstream_client::Target;
use eventstream_webhook_sink::{
    acknowledge, claim_abandoned, data_streams, decode_error, decode_record, deliver_with_retry,
    ensure_group, read_new, read_own_pending, sleep, RedisEntry, RetryPolicy, WebhookSink,
};

type AppResult<T> = Result<T, Box<dyn Error>>;

/// Forward Redis Event Stream entries to an HTTP webhook with at-least-once
/// delivery inside the Redis retention window.
#[derive(Parser)]
#[command(name = "eventstream-webhook-sink", version, about)]
struct Cli {
    /// Redis URL. This reference implementation supports standalone Redis.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    url: String,
    /// Module stream prefix.
    #[arg(long, default_value = "events:")]
    prefix: String,
    /// HTTP endpoint that accepts JSON POSTs.
    #[arg(long)]
    webhook: String,
    /// Consumer-group name unique to this downstream sink.
    #[arg(long, default_value = "eventstream-webhook")]
    group: String,
    /// Consumer name for this process.
    #[arg(long, default_value = "sink-1")]
    consumer: String,
    /// Entries idle this long can be claimed from a dead consumer.
    #[arg(long, default_value_t = 60_000)]
    min_idle_ms: u64,
    /// Maximum entries requested per Redis read or claim.
    #[arg(long, default_value_t = 100)]
    batch: usize,
    /// Maximum blocking time for each consumer-group read.
    #[arg(long, default_value_t = 1_000)]
    block_ms: u64,
    /// Refresh stream discovery at this interval.
    #[arg(long, default_value_t = 10)]
    refresh_secs: u64,
    /// HTTP request timeout.
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    /// Maximum delivery attempts per record; 0 retries forever.
    #[arg(long, default_value_t = 0)]
    max_attempts: u32,
    /// Initial retry delay.
    #[arg(long, default_value_t = 250)]
    retry_initial_ms: u64,
    /// Retry-delay cap.
    #[arg(long, default_value_t = 30_000)]
    retry_max_ms: u64,
    /// Exit after forwarding this many records; omitted means run forever.
    #[arg(long)]
    max_records: Option<u64>,
}

struct Worker {
    prefix: String,
    group: String,
    consumer: String,
    min_idle_ms: u64,
    batch: usize,
    max_records: Option<u64>,
    forwarded: u64,
    sink: WebhookSink,
    retry: RetryPolicy,
}

impl Worker {
    fn limit_reached(&self) -> bool {
        self.max_records
            .map(|limit| self.forwarded >= limit)
            .unwrap_or(false)
    }

    fn forward(
        &mut self,
        conn: &mut eventstream_client::Conn,
        entries: Vec<RedisEntry>,
    ) -> AppResult<()> {
        for entry in entries {
            if self.limit_reached() {
                break;
            }
            let record = decode_record(&self.prefix, &entry.stream, &entry.id, &entry.fields)
                .map_err(decode_error)?;
            let attempts = deliver_with_retry(
                &record,
                self.retry,
                |value| self.sink.deliver(value),
                sleep,
                |attempt, error, delay| {
                    eprintln!(
                        "delivery attempt {attempt} failed for {}: {error}; retrying in {}ms",
                        record.idempotency_key(),
                        delay.as_millis()
                    );
                },
            )
            .map_err(io::Error::other)?;
            let acked = acknowledge(conn, &entry.stream, &self.group, &entry.id)?;
            if acked == 0 {
                eprintln!(
                    "warning: {} delivered after its pending entry disappeared",
                    record.idempotency_key()
                );
            }
            self.forwarded += 1;
            println!(
                "forwarded {} (attempts={attempts}, total={})",
                record.idempotency_key(),
                self.forwarded
            );
        }
        Ok(())
    }

    fn recover_stream(
        &mut self,
        conn: &mut eventstream_client::Conn,
        stream: &str,
    ) -> AppResult<()> {
        loop {
            let pending = read_own_pending(conn, stream, &self.group, &self.consumer, self.batch)?;
            if pending.is_empty() {
                break;
            }
            self.forward(conn, pending)?;
            if self.limit_reached() {
                return Ok(());
            }
        }

        let mut cursor = "0-0".to_string();
        loop {
            let claimed = claim_abandoned(
                conn,
                stream,
                &self.group,
                &self.consumer,
                self.min_idle_ms,
                &cursor,
                self.batch,
            )?;
            for id in &claimed.deleted_ids {
                eprintln!("lost pending entry {stream}/{id}: trimmed before it could be claimed");
            }
            self.forward(conn, claimed.entries)?;
            if self.limit_reached() || claimed.next == "0-0" {
                break;
            }
            cursor = claimed.next;
        }
        Ok(())
    }

    fn add_streams(
        &mut self,
        conn: &mut eventstream_client::Conn,
        known: &mut BTreeSet<String>,
        candidates: Vec<String>,
    ) -> AppResult<()> {
        for stream in candidates {
            if known.contains(&stream) {
                continue;
            }
            ensure_group(conn, &stream, &self.group)?;
            println!("watching {stream} with group {}", self.group);
            known.insert(stream.clone());
            self.recover_stream(conn, &stream)?;
            if self.limit_reached() {
                break;
            }
        }
        Ok(())
    }
}

fn discovered_with_control(target: &Target) -> Vec<String> {
    let mut streams = data_streams(target);
    streams.push(format!("{}#control", target.prefix));
    streams.sort();
    streams.dedup();
    streams
}

fn run(cli: Cli) -> AppResult<()> {
    let target = Target::detect(&cli.url, &cli.prefix)?;
    if target.is_cluster {
        return Err(io::Error::other(
            "this reference sink is standalone-first; run one worker per cluster master \
             using the topology pattern in docs/src/topologies.md",
        )
        .into());
    }

    let mut conn = target.open_rw()?;
    let mut worker = Worker {
        prefix: cli.prefix,
        group: cli.group,
        consumer: cli.consumer,
        min_idle_ms: cli.min_idle_ms,
        batch: cli.batch.max(1),
        max_records: cli.max_records,
        forwarded: 0,
        sink: WebhookSink::new(
            cli.webhook,
            Duration::from_secs(cli.request_timeout_secs.max(1)),
        )?,
        retry: RetryPolicy {
            max_attempts: cli.max_attempts,
            initial_delay: Duration::from_millis(cli.retry_initial_ms.max(1)),
            max_delay: Duration::from_millis(cli.retry_max_ms.max(cli.retry_initial_ms.max(1))),
        },
    };

    let mut known = BTreeSet::new();
    worker.add_streams(&mut conn, &mut known, discovered_with_control(&target))?;
    let refresh_every = Duration::from_secs(cli.refresh_secs.max(1));
    let mut last_refresh = Instant::now();

    while !worker.limit_reached() {
        if last_refresh.elapsed() >= refresh_every {
            worker.add_streams(&mut conn, &mut known, discovered_with_control(&target))?;
            last_refresh = Instant::now();
        }
        let streams: Vec<String> = known.iter().cloned().collect();
        let entries = read_new(
            &mut conn,
            &streams,
            &worker.group,
            &worker.consumer,
            worker.batch,
            cli.block_ms,
        )?;
        worker.forward(&mut conn, entries)?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
