//! Web live-events demo (issue #113): a browser page showing captured events
//! lighting up live, fed by a tiny SSE bridge.
//!
//! Strictly consumer-side and read-only against Redis (XREAD / XLEN / INFO /
//! EVENTSTREAM.STREAMS / CLUSTER NODES, all via the `eventstream_client`
//! library): no XADD, no CONFIG SET, no consumer groups. It reuses the
//! library's discovery and merge rather than inventing a third mechanism, so a
//! per-node cluster and post-reshard streams appear the same way the shipped
//! consumer sees them (issue #215). Driving the demo is the existing
//! `eventstream-client produce` command's job, not this bridge's.
//!
//! Run:
//! ```text
//! cargo run -p eventstream-client --example eventstream_web -- --listen 127.0.0.1:8080
//! # then open http://127.0.0.1:8080 and, in another terminal:
//! eventstream-client produce --expire 50 --toggle
//! ```

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use eventstream_client::{counter_sum, discover_all, read_gap_markers, MergedReader, Target};

/// The static page, embedded so the demo is "the module plus one binary".
const INDEX_HTML: &str = include_str!("eventstream_web.html");

struct Args {
    url: String,
    prefix: String,
    listen: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        url: "redis://127.0.0.1:6379".to_string(),
        prefix: "events:".to_string(),
        listen: "127.0.0.1:8080".to_string(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--url" => a.url = it.next().unwrap_or(a.url),
            "--prefix" => a.prefix = it.next().unwrap_or(a.prefix),
            "--listen" => a.listen = it.next().unwrap_or(a.listen),
            "-h" | "--help" => {
                eprintln!(
                    "usage: eventstream_web [--url redis://host:port] [--prefix events:] \
                     [--listen 127.0.0.1:8080]"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    a
}

fn main() {
    let args = parse_args();
    // Fail fast if the target is unreachable, before binding the listener.
    if let Err(e) = Target::detect(&args.url, &args.prefix) {
        eprintln!("error: connect to {}: {e}", args.url);
        std::process::exit(1);
    }
    let listener = match TcpListener::bind(&args.listen) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: bind {}: {e}", args.listen);
            std::process::exit(1);
        }
    };
    println!(
        "eventstream web demo on http://{} (target {}, prefix {})",
        args.listen, args.url, args.prefix
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let url = args.url.clone();
        let prefix = args.prefix.clone();
        // One thread per connection: the SSE loop holds its connection open,
        // so it must not block the accept loop or other clients.
        thread::spawn(move || handle(stream, &url, &prefix));
    }
}

/// Route one connection by its request line. Only two paths exist: the page
/// and the SSE feed; anything else is a 404.
fn handle(mut stream: TcpStream, url: &str, prefix: &str) {
    let path = {
        let mut reader = BufReader::new(&stream);
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() {
            return;
        }
        // "GET /events HTTP/1.1" -> "/events".
        line.split_whitespace().nth(1).unwrap_or("/").to_string()
    };
    match path.as_str() {
        "/" => serve_page(&mut stream),
        "/events" => serve_events(stream, url, prefix),
        _ => {
            let _ = stream.write_all(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    }
}

fn serve_page(stream: &mut TcpStream) {
    let body = INDEX_HTML.as_bytes();
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
}

/// Stream Server-Sent Events until the client disconnects (a failed write ends
/// the loop). Each connection gets its own Redis connection, merged reader, and
/// cursors, so clients are independent.
fn serve_events(mut stream: TcpStream, url: &str, prefix: &str) {
    let header = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\
         Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
    if stream.write_all(header.as_bytes()).is_err() {
        return;
    }

    let Ok(target) = Target::detect(url, prefix) else {
        let _ = send(&mut stream, "error", "{\"message\":\"cannot reach Redis\"}");
        return;
    };
    let Ok(mut conn) = target.open_rw() else {
        return;
    };

    // Data streams only for the live lanes (skip the module's own `#` keys:
    // #control is surfaced as markers, #streams is the registry, #firehose is
    // a combined copy that would double every event).
    let data_streams = |t: &Target| -> Vec<String> {
        discover_all(t)
            .into_iter()
            .filter(|s| !s.contains('#'))
            .collect()
    };

    // Start at the tail: the demo shows events as they happen, not history.
    let mut reader = MergedReader::new(&mut conn, data_streams(&target), false);
    // Gap markers: start after the newest existing marker so the band only
    // appears for gaps that open while the page is watching.
    let mut marker_from = newest_marker_id(&target);

    let mut last_discovery = Instant::now();
    let mut last_stats = Instant::now();
    loop {
        // Entries.
        for e in reader.poll(&mut conn, 100) {
            let json = format!(
                "{{\"stream\":{},\"id\":{},\"event\":{},\"key\":{},\"db\":{}}}",
                jstr(&e.stream),
                jstr(&e.id),
                jstr(&e.event),
                jstr(&e.key),
                jstr(&e.db),
            );
            if send(&mut stream, "entry", &json).is_err() {
                return;
            }
        }

        // Gap markers (capture off/on bands).
        if let Ok(markers) = read_gap_markers(&target, &marker_from) {
            for m in markers {
                if m.id.as_str() > marker_from.as_str() {
                    marker_from = m.id.clone();
                }
                let db = m.db.map(|d| d.to_string()).unwrap_or_else(|| "null".into());
                let json = format!(
                    "{{\"stream\":{},\"id\":{},\"action\":{},\"db\":{}}}",
                    jstr(&m.stream),
                    jstr(&m.id),
                    jstr(&m.action),
                    db,
                );
                if send(&mut stream, "marker", &json).is_err() {
                    return;
                }
            }
        }

        // Stats tiles, every ~1.5s.
        if last_stats.elapsed() >= Duration::from_millis(1500) {
            last_stats = Instant::now();
            let forwarded = counter_sum(&target, "forwarded");
            let events_lost = counter_sum(&target, "events_lost");
            let dropped = counter_sum(&target, "dropped");
            let total_len: i64 = reader
                .streams()
                .iter()
                .map(|s| redis::cmd("XLEN").arg(s).query(&mut conn).unwrap_or(0))
                .sum();
            let json = format!(
                "{{\"forwarded\":{forwarded},\"events_lost\":{events_lost},\
                 \"dropped\":{dropped},\"streams\":{},\"length\":{total_len}}}",
                reader.streams().len()
            );
            if send(&mut stream, "stats", &json).is_err() {
                return;
            }
        }

        // Pick up new event types and re-pinned cluster streams without a
        // restart (issue #215): cursors of streams already read are preserved.
        // A stream that appears mid-session (a first-ever event of some type,
        // or a fresh server whose streams did not exist at connect) is read
        // from 0 so its events show, unlike the pre-existing streams the
        // initial reader started at the tail to avoid dumping old history.
        if last_discovery.elapsed() >= Duration::from_secs(2) {
            last_discovery = Instant::now();
            reader.add_streams(&mut conn, &data_streams(&target), true);
        }

        thread::sleep(Duration::from_millis(200));
    }
}

/// The id of the newest marker currently on any control stream, or `"0"` if
/// none, so a fresh page starts reading markers from the present.
fn newest_marker_id(target: &Target) -> String {
    read_gap_markers(target, "0")
        .ok()
        .and_then(|ms| ms.into_iter().map(|m| m.id).max())
        .unwrap_or_else(|| "0".to_string())
}

/// Write one SSE frame. `Err` means the client went away and the loop should
/// stop.
fn send(stream: &mut TcpStream, event: &str, data: &str) -> std::io::Result<()> {
    stream.write_all(format!("event: {event}\ndata: {data}\n\n").as_bytes())
}

/// A JSON string literal (quotes included), escaping the characters JSON
/// requires. `Entry`/`GapMarker` fields are already lossy-decoded (non-UTF-8
/// key bytes became the U+FFFD marker), so this only needs to make the text
/// JSON-safe, not byte-safe.
fn jstr(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
