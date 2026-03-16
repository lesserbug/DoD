// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::collections::HashSet;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Narwhal and Tusk.")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--client-id=<INT> 'A unique identifier for this benchmark client'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let client_id = matches
        .value_of("client-id")
        .unwrap_or("0")
        .parse::<u16>()
        .context("The client id must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);

    let client = Client {
        target,
        size,
        rate,
        client_id,
        nodes,
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
    size: usize,
    rate: u64,
    client_id: u16,
    nodes: Vec<SocketAddr>,
}

impl Client {
    fn sample_id(&self, counter: u64) -> u64 {
        ((self.client_id as u64) << 48) | (counter & 0x0000_FFFF_FFFF_FFFF)
    }

    pub async fn send(&self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        // At least: 1(kind) + 8(tx id) + 1(state key).
        if self.size < 10 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 10 bytes for DoD Protocol",
            ));
        }

        // Build a de-duplicated target list while preserving order.
        let mut seen = HashSet::new();
        let mut all_targets = Vec::new();
        for address in std::iter::once(self.target).chain(self.nodes.iter().copied()) {
            if seen.insert(address) {
                all_targets.push(address);
            }
        }

        let mut transports = Vec::new();
        for address in all_targets {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    transports.push((address, Framed::new(stream, LengthDelimitedCodec::new())));
                }
                Err(e) => warn!("Failed to connect to replica {}: {}", address, e),
            }
        }

        if transports.is_empty() {
            return Err(anyhow::Error::msg("Failed to connect to any replica"));
        }

        info!(
            "DoD Protocol: Broadcasting to {} replicas",
            transports.len()
        );

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        if burst == 0 {
            return Err(anyhow::Error::msg(
                "Transaction rate must be at least 20 tx/s",
            ));
        }

        let mut tx = BytesMut::with_capacity(self.size);
        let mut counter = 0;
        let mut r: u64 = rand::thread_rng().gen();
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");

        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            for x in 0..burst {
                if x == counter % burst {
                    let sample_id = self.sample_id(counter);
                    // NOTE: This log entry is used to compute performance.
                    info!("Sending sample transaction {}", sample_id);

                    tx.put_u8(0u8); // Sample txs start with 0.
                    tx.put_u64(sample_id); // This identifies the tx globally.
                    tx.put_u8(0u8); // Keep a fixed-length DoD transaction layout.
                } else {
                    r = r.wrapping_add(1);
                    tx.put_u8(1u8); // Standard txs start with 1.
                    tx.put_u64(r); // Unique transaction id.
                    tx.put_u8((r % 100) as u8); // Synthetic key for conflicts.
                };

                tx.resize(self.size, 0u8);
                let bytes = tx.split().freeze();

                let mut failed = Vec::new();
                for (i, (address, transport)) in transports.iter_mut().enumerate() {
                    if let Err(e) = transport.send(bytes.clone()).await {
                        warn!("Failed to broadcast transaction to {}: {}", address, e);
                        failed.push(i);
                    }
                }

                // Drop dead connections to avoid warning on every future transaction.
                for i in failed.into_iter().rev() {
                    transports.swap_remove(i);
                }

                if transports.is_empty() {
                    warn!("All broadcast connections failed, stopping client");
                    break 'main;
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
