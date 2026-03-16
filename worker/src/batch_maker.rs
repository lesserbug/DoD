// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
use std::collections::HashMap;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;

#[derive(Clone, serde::Serialize, serde::Deserialize, Default, Debug, PartialEq, Eq)]
pub struct Batch {
    pub author: PublicKey,
    pub sequence: u64,
    pub transactions: Vec<Transaction>,
    // Local-order dependency edges: (previous_tx_id, current_tx_id).
    // We keep every earlier conflicting transaction, letting later global
    // pruning remove redundant edges.
    pub edges: Vec<(u64, u64)>,
}

pub(crate) fn parse_transaction_id_and_state_key(tx: &[u8]) -> Option<(u64, u8)> {
    // We accept both DoD benchmark transaction layouts:
    //  - sample   : [kind=0][tx_id:8] (legacy 9-byte prefix)
    //  - standard : [kind=1][tx_id:8][state_key:1]
    if tx.len() < 9 {
        return None;
    }

    let mut id_bytes = [0u8; 8];
    id_bytes.copy_from_slice(&tx[1..9]);
    let tx_id = u64::from_be_bytes(id_bytes);
    let state_key = if tx.len() >= 10 { tx[9] } else { 0u8 };
    Some((tx_id, state_key))
}

pub(crate) fn parse_standard_transaction(tx: &[u8]) -> Option<(u64, u8)> {
    // Benchmark client format:
    // [0]    : transaction kind (1 for standard transactions)
    // [1..9] : transaction id (u64, big-endian)
    // [9]    : synthetic state key to simulate conflicts
    if tx.first() != Some(&1u8) || tx.len() < 10 {
        return None;
    }

    parse_transaction_id_and_state_key(tx)
}

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The public key of this authority.
    name: PublicKey,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Vec<Transaction>,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    /// Records all prior transaction ids seen for each key so local-order
    /// graphs can connect a new transaction to every earlier dependency.
    writers_by_key: HashMap<u8, Vec<u64>>,
    /// Sequence number of the next local-order graph.
    next_sequence: u64,
}

impl BatchMaker {
    pub fn spawn(
        name: PublicKey,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                batch_size,
                max_batch_delay,
                rx_transaction,
                tx_message,
                workers_addresses,
                current_batch: Vec::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                writers_by_key: HashMap::new(),
                next_sequence: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }

            // Give the chance to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.first() == Some(&0u8) && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        self.current_batch_size = 0;
        let transactions: Vec<_> = self.current_batch.drain(..).collect();

        let mut edges = Vec::new();
        for tx in &transactions {
            if let Some((tx_id, state_key)) = parse_standard_transaction(tx) {
                let prior_writers = self.writers_by_key.entry(state_key).or_default();
                for &prev_tx_id in prior_writers.iter() {
                    if prev_tx_id != tx_id {
                        edges.push((prev_tx_id, tx_id));
                    }
                }

                if !prior_writers.contains(&tx_id) {
                    prior_writers.push(tx_id);
                }
            }
        }

        let batch = Batch {
            author: self.name,
            sequence: self.next_sequence,
            transactions,
            edges,
        };
        self.next_sequence += 1;

        let message = WorkerMessage::LocalBatch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: Kept for debugging local-order traffic; benchmark parser ignores this prefix.
                info!(
                    "LocalGraph {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: Kept for debugging local-order traffic; benchmark parser ignores this prefix.
            info!("LocalGraph {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.
        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Send the batch through the deliver channel for further processing.
        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }
}
