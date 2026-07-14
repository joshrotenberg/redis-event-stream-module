//! Consumer/verification client for redis-event-stream-module.
//!
//! Drives events into the module and reads the mirrored streams back, against a
//! standalone server or a per-node cluster (auto-detected). It is the runnable
//! consumer reference the cluster docs point to and the driver for the chaos
//! suite's soak/produce scenarios.
//!
//! The consumer logic (discovery fan-out, merged reader, gap markers) lives in
//! the `eventstream_client` library; this binary is the command-line surface
//! over it.

use std::collections::HashSet;
use std::thread::sleep;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use redis::{Cmd, RedisResult};

use eventstream_client::{
    counter_sum, discover_streams, node_counters, Conn, MergedReader, Target,
};

/// Consumer/verification client for redis-event-stream-module.
#[derive(Parser)]
#[command(name = "eventstream-client", version, about)]
struct Cli {
    /// Server URL to connect to (a `redis://` scheme is added if absent).
    #[arg(long, global = true, default_value = "redis://127.0.0.1:6379")]
    url: String,
    /// Destination stream prefix; must match the module's `stream-prefix`.
    #[arg(long, global = true, default_value = "events:")]
    prefix: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Topology, each master's module counters, and discovered streams.
    Info,
    /// Drive events into the module.
    Produce {
        /// Fire N `set` events via N `SET`s.
        #[arg(long, default_value_t = 0)]
        sets: i64,
        /// Set N keys with a TTL, then force their expiry.
        #[arg(long, default_value_t = 0)]
        expire: i64,
        /// Mass-expiry: N keys with tiny TTLs, forced.
        #[arg(long, default_value_t = 0)]
        burst: i64,
        /// TTL in milliseconds for --expire / --burst keys.
        #[arg(long = "ttl-ms", default_value_t = 200)]
        ttl_ms: i64,
        /// Flip eventstream.enabled off then on (writes a gap-marker pair).
        #[arg(long, default_value_t = false)]
        toggle: bool,
    },
    /// Discover streams cluster-wide and tail them merged by entry ID.
    Consume {
        /// Only these event types (comma list; default: all discovered).
        #[arg(long)]
        events: Option<String>,
        /// Start at the beginning (`0`) or only new entries (`$`, default).
        #[arg(long, default_value = "$")]
        from: String,
        /// Stop after N entries (default: run until Ctrl-C).
        #[arg(long)]
        count: Option<i64>,
    },
    /// Live dashboard of counters and stream lengths.
    Watch,
    /// Sustained produce, then verify capture.
    Soak {
        /// Events to drive.
        #[arg(long, default_value_t = 5000)]
        count: i64,
        /// Cap at N events/sec (0 = unlimited).
        #[arg(long, default_value_t = 0)]
        rate: i64,
    },
}

fn main() {
    let cli = Cli::parse();
    let target = match Target::detect(&cli.url, &cli.prefix) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("error: connect to {}: {e}", cli.url);
            std::process::exit(1);
        }
    };
    let result = match &cli.command {
        Command::Info => cmd_info(&target),
        Command::Produce {
            sets,
            expire,
            burst,
            ttl_ms,
            toggle,
        } => cmd_produce(&target, *sets, *expire, *burst, *ttl_ms, *toggle),
        Command::Consume {
            events,
            from,
            count,
        } => cmd_consume(&target, events.as_deref(), from, *count),
        Command::Watch => cmd_watch(&target),
        Command::Soak { count, rate } => cmd_soak(&target, *count, *rate),
    };
    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// info
// ---------------------------------------------------------------------------

fn cmd_info(target: &Target) -> RedisResult<()> {
    println!(
        "topology: {} ({} master{})",
        if target.is_cluster {
            "cluster (per-node)"
        } else {
            "standalone"
        },
        target.masters.len(),
        if target.masters.len() == 1 { "" } else { "s" }
    );
    for m in &target.masters {
        let c = node_counters(m);
        let get = |k: &str| c.get(k).cloned().unwrap_or_else(|| "-".to_string());
        println!(
            "  {m}: forwarded={} dropped={} repins={} enabled={} tag={}",
            get("forwarded"),
            get("dropped"),
            get("repins"),
            get("enabled"),
            get("cluster_pinned_tag")
        );
    }
    let streams = discover_streams(target);
    println!("streams ({}):", streams.len());
    let mut conn = target.open_rw()?;
    for s in &streams {
        let len: i64 = Cmd::new().arg("XLEN").arg(s).query(&mut conn).unwrap_or(0);
        println!("  {s}  (len {len})");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// produce
// ---------------------------------------------------------------------------

/// Widen the module's event filter on every master so what we produce is
/// captured, without narrowing an existing filter.
fn ensure_events(target: &Target, want: &[&str]) {
    for m in &target.masters {
        let Ok(mut c) = eventstream_client::open_single(m) else {
            continue;
        };
        let current: Vec<String> = Cmd::new()
            .arg("CONFIG")
            .arg("GET")
            .arg("eventstream.events")
            .query(&mut c)
            .unwrap_or_default();
        let current = current.get(1).cloned().unwrap_or_default();
        if current == "*" {
            continue;
        }
        let mut have: HashSet<String> = current.split(',').map(|s| s.trim().to_string()).collect();
        let before = have.len();
        for w in want {
            have.insert((*w).to_string());
        }
        if have.len() != before {
            let joined = have.into_iter().collect::<Vec<_>>().join(",");
            let _: RedisResult<()> = Cmd::new()
                .arg("CONFIG")
                .arg("SET")
                .arg("eventstream.events")
                .arg(&joined)
                .query(&mut c);
        }
    }
}

/// Set the module's event filter to exactly `value` on every master. Used by
/// soak so each produced SET yields exactly one captured event, making the
/// forwarded count directly comparable to the produced count.
fn set_events(target: &Target, value: &str) {
    for m in &target.masters {
        let Ok(mut c) = eventstream_client::open_single(m) else {
            continue;
        };
        let _: RedisResult<()> = Cmd::new()
            .arg("CONFIG")
            .arg("SET")
            .arg("eventstream.events")
            .arg(value)
            .query(&mut c);
    }
}

fn cmd_produce(
    target: &Target,
    sets: i64,
    expire: i64,
    burst: i64,
    ttl_ms: i64,
    toggle: bool,
) -> RedisResult<()> {
    let ttl_ms = ttl_ms.max(1);
    if sets == 0 && expire == 0 && burst == 0 && !toggle {
        println!("nothing to do; pass --sets N, --expire N, --burst N, or --toggle");
        return Ok(());
    }

    let mut needed = vec![];
    if sets > 0 {
        needed.push("set");
    }
    if expire > 0 || burst > 0 {
        needed.push("expired");
    }
    ensure_events(target, &needed);
    let mut conn = target.open_rw()?;

    if sets > 0 {
        for i in 0..sets {
            let _: () = Cmd::new()
                .arg("SET")
                .arg(format!("demo:set:{i}"))
                .arg("v")
                .query(&mut conn)?;
        }
        println!("produced {sets} set events");
    }

    if expire > 0 {
        force_expiry(&mut conn, "demo:exp", expire, ttl_ms)?;
        println!("produced {expire} expired events (ttl {ttl_ms}ms)");
    }

    if burst > 0 {
        let start = Instant::now();
        force_expiry(&mut conn, "demo:burst", burst, ttl_ms)?;
        let secs = start.elapsed().as_secs_f64().max(0.001);
        println!(
            "burst: {burst} expirations in {:.2}s ({:.0}/s attempted)",
            secs,
            burst as f64 / secs
        );
    }

    if toggle {
        toggle_enabled(target)?;
        println!("toggled eventstream.enabled off then on (a gap-marker pair)");
    }
    Ok(())
}

/// Set `n` keys with a TTL, then read them back after the TTL so lazy expiry
/// fires promptly even if the active cycle has not reached them. Keys are not
/// hash-tagged, so in cluster mode they spread across masters and every node
/// captures its share.
fn force_expiry(conn: &mut Conn, label: &str, n: i64, ttl_ms: i64) -> RedisResult<()> {
    for i in 0..n {
        let _: () = Cmd::new()
            .arg("SET")
            .arg(format!("{label}:{i}"))
            .arg("v")
            .arg("PX")
            .arg(ttl_ms)
            .query(conn)?;
    }
    sleep(Duration::from_millis(ttl_ms as u64 + 50));
    for i in 0..n {
        let _: Option<String> = Cmd::new()
            .arg("GET")
            .arg(format!("{label}:{i}"))
            .query(conn)?;
    }
    Ok(())
}

/// Flip `eventstream.enabled` off then on across all masters, writing a key in
/// between whose event is lost, so the control stream gets a disabled/enabled
/// marker pair.
fn toggle_enabled(target: &Target) -> RedisResult<()> {
    let mut conn = target.open_rw()?;
    for m in &target.masters {
        let mut c = eventstream_client::open_single(m)?;
        let _: () = Cmd::new()
            .arg("CONFIG")
            .arg("SET")
            .arg("eventstream.enabled")
            .arg("no")
            .query(&mut c)?;
    }
    let _: () = Cmd::new()
        .arg("SET")
        .arg("demo:lost")
        .arg("v")
        .query(&mut conn)?;
    for m in &target.masters {
        let mut c = eventstream_client::open_single(m)?;
        let _: () = Cmd::new()
            .arg("CONFIG")
            .arg("SET")
            .arg("eventstream.enabled")
            .arg("yes")
            .query(&mut c)?;
    }
    // One more write so the `enabled` marker lands (it timestamps the first
    // event after re-enabling).
    let _: () = Cmd::new()
        .arg("SET")
        .arg("demo:resumed")
        .arg("v")
        .query(&mut conn)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// consume
// ---------------------------------------------------------------------------

fn cmd_consume(
    target: &Target,
    events: Option<&str>,
    from: &str,
    count: Option<i64>,
) -> RedisResult<()> {
    // A set of event names, e.g. {"set", "expired"}.
    let wanted: Option<HashSet<String>> =
        events.map(|s| s.split(',').map(|e| e.trim().to_string()).collect());
    let from_zero = from == "0";
    let limit = count.unwrap_or(i64::MAX);

    let mut streams = discover_streams(target);
    if let Some(w) = &wanted {
        // Match by event name, which in cluster mode sits after the {tag}.
        streams.retain(|s| {
            eventstream_client::event_name(&target.prefix, s).is_some_and(|e| w.contains(&e))
        });
    }
    // Skip the module's own control/registry keys.
    streams.retain(|s| !s.contains('#'));
    if streams.is_empty() {
        println!("no matching streams yet; produce some events first, then re-run");
        return Ok(());
    }
    println!(
        "tailing {} stream(s): {}",
        streams.len(),
        streams.join(", ")
    );

    let mut conn = target.open_rw()?;
    let mut reader = MergedReader::new(&mut conn, streams, from_zero);

    let mut seen = 0i64;
    loop {
        for e in reader.poll(&mut conn, 200) {
            println!("{e}");
            seen += 1;
            if seen >= limit {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(200));
    }
}

// ---------------------------------------------------------------------------
// watch
// ---------------------------------------------------------------------------

fn cmd_watch(target: &Target) -> RedisResult<()> {
    loop {
        // Clear screen, home cursor.
        print!("\x1b[2J\x1b[H");
        println!(
            "eventstream watch  ({})  Ctrl-C to exit",
            if target.is_cluster {
                "cluster"
            } else {
                "standalone"
            }
        );
        println!();
        println!(
            "{:<22} {:>10} {:>8} {:>7} {:>8} {:>8}",
            "node", "forwarded", "dropped", "repins", "enabled", "tag"
        );
        for m in &target.masters {
            let c = node_counters(m);
            let g = |k: &str| c.get(k).cloned().unwrap_or_else(|| "-".to_string());
            println!(
                "{:<22} {:>10} {:>8} {:>7} {:>8} {:>8}",
                m,
                g("forwarded"),
                g("dropped"),
                g("repins"),
                g("enabled"),
                g("cluster_pinned_tag")
            );
        }
        println!();
        let streams = discover_streams(target);
        let mut conn = target.open_rw()?;
        println!("{:<28} {:>10}", "stream", "length");
        for s in &streams {
            let len: i64 = Cmd::new().arg("XLEN").arg(s).query(&mut conn).unwrap_or(0);
            println!("{s:<28} {len:>10}");
        }
        sleep(Duration::from_millis(1000));
    }
}

// ---------------------------------------------------------------------------
// soak
// ---------------------------------------------------------------------------

fn cmd_soak(target: &Target, count: i64, rate: i64) -> RedisResult<()> {
    let count = count.max(1);
    // Capture exactly `set` so each produced SET yields one captured event and
    // the forwarded delta is directly comparable to the produced count.
    set_events(target, "set");
    println!("soak: set eventstream.events=set on all masters for an exact count");

    let forwarded_before = counter_sum(target, "forwarded");
    let mut conn = target.open_rw()?;

    println!(
        "soak: producing {count} set events{}...",
        if rate > 0 {
            format!(" at up to {rate}/s")
        } else {
            String::new()
        }
    );
    let start = Instant::now();
    let min_per = if rate > 0 {
        Duration::from_secs_f64(1.0 / rate as f64)
    } else {
        Duration::ZERO
    };
    for i in 0..count {
        let iter_start = Instant::now();
        let _: () = Cmd::new()
            .arg("SET")
            .arg(format!("soak:{i}"))
            .arg("v")
            .query(&mut conn)?;
        if i % 1000 == 999 {
            println!("  produced {}/{count}", i + 1);
        }
        if !min_per.is_zero() {
            if let Some(rem) = min_per.checked_sub(iter_start.elapsed()) {
                sleep(rem);
            }
        }
    }
    let produce_secs = start.elapsed().as_secs_f64();
    println!(
        "produced {count} in {:.2}s ({:.0}/s); waiting for capture to settle...",
        produce_secs,
        count as f64 / produce_secs.max(0.001)
    );

    // Wait until forwarded stops rising.
    let mut last = -1i64;
    let mut stable_since = Instant::now();
    loop {
        let now = counter_sum(target, "forwarded") - forwarded_before;
        if now != last {
            last = now;
            stable_since = Instant::now();
        } else if stable_since.elapsed() > Duration::from_secs(2) {
            break;
        }
        if start.elapsed() > Duration::from_secs(120) {
            println!("  (giving up waiting after 120s)");
            break;
        }
        sleep(Duration::from_millis(200));
    }

    let captured = counter_sum(target, "forwarded") - forwarded_before;
    let dropped = counter_sum(target, "dropped");
    let repins = counter_sum(target, "repins");
    println!("---");
    println!("produced : {count}");
    println!(
        "captured : {captured}  ({:.2}%)",
        100.0 * captured as f64 / count as f64
    );
    println!("dropped  : {dropped} (cumulative, all reasons)");
    if target.is_cluster {
        println!("repins   : {repins} (cumulative)");
    }
    if captured >= count {
        println!("result   : OK, every produced event was captured");
    } else {
        println!(
            "result   : {} event(s) not captured (see dropped_* counters via `info`)",
            count - captured
        );
    }
    Ok(())
}
