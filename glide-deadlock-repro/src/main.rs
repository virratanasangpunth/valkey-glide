use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use glide_core::client::Client;

#[derive(Parser)]
#[command(name = "glide-deadlock-repro")]
#[command(about = "Reproduces TCP deadlock bug using glide-core Client")]
struct Args {
    /// Redis/Valkey server host
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Redis/Valkey server port
    #[arg(long, default_value_t = 6379)]
    port: u16,

    /// Disable TLS (default: TLS enabled with InsecureTls mode)
    #[arg(long)]
    no_tls: bool,

    /// Payload size in bytes for SET operations
    #[arg(long, default_value_t = 10_485_760)]
    payload_size: usize,

    /// Number of concurrent writer tasks
    #[arg(long, default_value_t = 8)]
    writers: usize,

    /// Number of concurrent reader tasks
    #[arg(long, default_value_t = 4)]
    readers: usize,

    /// Per-operation timeout in seconds (deadlock detection)
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,

    /// Total test duration in seconds
    #[arg(long, default_value_t = 120)]
    duration_secs: u64,
}

struct Counters {
    write_successes: AtomicU64,
    read_successes: AtomicU64,
    write_deadlocks: AtomicU64,
    read_deadlocks: AtomicU64,
}

#[allow(clippy::expect_used)]
#[tokio::main]
async fn main() {
    rustls::crypto::CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider())
        .expect("Failed to install default CryptoProvider");
    let args = Args::parse();

    let tls_mode = if args.no_tls {
        None
    } else {
        Some(glide_core::client::TlsMode::InsecureTls)
    };

    println!(
        "glide-deadlock-repro: host={} port={} tls={:?} payload_size={} writers={} readers={} timeout={}s duration={}s",
        args.host,
        args.port,
        tls_mode,
        args.payload_size,
        args.writers,
        args.readers,
        args.timeout_secs,
        args.duration_secs,
    );

    let connection_request = glide_core::ConnectionRequest {
        addresses: vec![glide_core::client::NodeAddress {
            host: args.host.clone(),
            port: args.port,
        }],
        tls_mode,
        // Set a very large internal timeout (10 min) so it doesn't interfere
        // with our external tokio::time::timeout-based deadlock detection.
        request_timeout: Some(600_000),
        ..Default::default()
    };

    let client = Client::new(connection_request, None)
        .await
        .expect("Failed to create glide-core client");

    println!("Connected via glide-core Client (single connection, no pool)");

    let payload = vec![b'X'; args.payload_size];
    let op_timeout = Duration::from_secs(args.timeout_secs);
    let test_duration = Duration::from_secs(args.duration_secs);
    let shutdown = tokio::time::Instant::now() + test_duration;

    let counters = Arc::new(Counters {
        write_successes: AtomicU64::new(0),
        read_successes: AtomicU64::new(0),
        write_deadlocks: AtomicU64::new(0),
        read_deadlocks: AtomicU64::new(0),
    });

    let mut handles = Vec::new();

    // Spawn writer tasks
    for id in 0..args.writers {
        let mut client = client.clone();
        let payload = payload.clone();
        let counters = counters.clone();
        let key = format!("deadlock-repro:writer-{id}");

        handles.push(tokio::spawn(async move {
            let mut iteration = 0u64;
            while tokio::time::Instant::now() < shutdown {
                iteration += 1;
                let mut cmd = redis::cmd("SET");
                cmd.arg(key.as_bytes()).arg(payload.as_slice());
                match tokio::time::timeout(op_timeout, client.send_command(&mut cmd, None)).await {
                    Ok(Ok(_)) => {
                        counters.write_successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(e)) => {
                        eprintln!("writer-{id} iter={iteration} redis error: {e}");
                    }
                    Err(_) => {
                        counters.write_deadlocks.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            ">>> DEADLOCK DETECTED <<< writer-{id} iter={iteration} SET timed out after {}s",
                            op_timeout.as_secs()
                        );
                    }
                }
            }
            println!("writer-{id} finished");
        }));
    }

    // Spawn reader tasks (slight delay to let writers fill buffers)
    for id in 0..args.readers {
        let mut client = client.clone();
        let counters = counters.clone();
        let num_writers = args.writers;

        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut iteration = 0u64;
            while tokio::time::Instant::now() < shutdown {
                iteration += 1;
                let writer_idx = (iteration as usize) % num_writers;
                let key = format!("deadlock-repro:writer-{writer_idx}");
                let mut cmd = redis::cmd("GET");
                cmd.arg(key.as_bytes());
                match tokio::time::timeout(op_timeout, client.send_command(&mut cmd, None)).await {
                    Ok(Ok(_)) => {
                        counters.read_successes.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(e)) => {
                        eprintln!("reader-{id} iter={iteration} redis error: {e}");
                    }
                    Err(_) => {
                        counters.read_deadlocks.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            ">>> DEADLOCK DETECTED <<< reader-{id} iter={iteration} GET timed out after {}s",
                            op_timeout.as_secs()
                        );
                    }
                }
            }
            println!("reader-{id} finished");
        }));
    }

    // Status monitor
    let counters_monitor = counters.clone();
    let monitor = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if tokio::time::Instant::now() >= shutdown {
                break;
            }
            let remaining = shutdown.saturating_duration_since(tokio::time::Instant::now());
            println!(
                "STATUS: writes_ok={} reads_ok={} write_deadlocks={} read_deadlocks={} remaining={remaining:.0?}",
                counters_monitor.write_successes.load(Ordering::Relaxed),
                counters_monitor.read_successes.load(Ordering::Relaxed),
                counters_monitor.write_deadlocks.load(Ordering::Relaxed),
                counters_monitor.read_deadlocks.load(Ordering::Relaxed),
            );
        }
    });

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }
    monitor.abort();

    // Final report
    let write_ok = counters.write_successes.load(Ordering::Relaxed);
    let read_ok = counters.read_successes.load(Ordering::Relaxed);
    let write_dl = counters.write_deadlocks.load(Ordering::Relaxed);
    let read_dl = counters.read_deadlocks.load(Ordering::Relaxed);
    let total_deadlocks = write_dl + read_dl;

    println!("=== FINAL REPORT ===");
    println!("Write successes: {write_ok}");
    println!("Read successes:  {read_ok}");
    println!("Write deadlocks: {write_dl}");
    println!("Read deadlocks:  {read_dl}");

    if total_deadlocks > 0 {
        eprintln!("RESULT: DEADLOCK BUG REPRODUCED ({total_deadlocks} deadlocks detected)");
    } else {
        println!("RESULT: No deadlocks detected");
    }
}
