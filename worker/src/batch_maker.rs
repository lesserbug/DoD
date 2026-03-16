// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use config::Stake;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
#[cfg(feature = "benchmark")]
use log::info;
use network::ReliableSender;
use std::collections::{HashMap, HashSet, VecDeque};
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
    // Within one local graph we keep every earlier conflicting transaction.
    // Across graphs we keep a bounded recent local window and bounded order
    // hints instead of a single persistent predecessor.
    pub edges: Vec<(u64, u64)>,
    #[serde(default)]
    pub missing_edges: Vec<(u64, u64)>,
}

/// A weak cross-round order signal exported by the global orderer.
///
/// These hints stay local to the worker and are intentionally bounded so we
/// can move toward DoD's `M_w` without reintroducing long-lived state that
/// hurts throughput.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderHint {
    pub predecessor: u64,
    pub successor: u64,
    pub state_key: u8,
    pub weight: Stake,
    pub observed_at_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchMakerControl {
    MergeOrderHints(Vec<OrderHint>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TrackedTx {
    tx_id: u64,
    sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ParsedStandardTx {
    tx_id: u64,
    state_key: u8,
    index: usize,
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
    /// Local control channel used by the global orderer to feed bounded `M_w`
    /// hints back into the batch maker.
    rx_control: Receiver<BatchMakerControl>,
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
    /// Bounded recent local transactions per state key. This is the minimal
    /// `S`-like window we retain locally instead of a single `last_writer`.
    recent_local_txs: HashMap<u8, VecDeque<TrackedTx>>,
    /// Bounded local approximation of the paper's `M_w`, keyed by successor.
    order_hints: HashMap<u64, Vec<OrderHint>>,
    /// Sequence number of the next local-order graph.
    next_sequence: u64,
}

impl BatchMaker {
    const MAX_RECENT_TXS_PER_KEY: usize = 32;
    const MAX_LOCAL_HISTORY_SEQUENCES: u64 = 32;
    const MAX_HINTS_PER_TX: usize = 8;
    const MAX_HINT_HISTORY_SEQUENCES: u64 = 32;
    const MAX_HINT_TARGETS: usize = 2_048;

    pub fn spawn(
        name: PublicKey,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        rx_control: Receiver<BatchMakerControl>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                batch_size,
                max_batch_delay,
                rx_transaction,
                rx_control,
                tx_message,
                workers_addresses,
                current_batch: Vec::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: ReliableSender::new(),
                recent_local_txs: HashMap::new(),
                order_hints: HashMap::new(),
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

                Some(control) = self.rx_control.recv() => {
                    self.handle_control(control);
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

        while let Ok(control) = self.rx_control.try_recv() {
            self.handle_control(control);
        }

        self.current_batch_size = 0;
        let transactions: Vec<_> = self.current_batch.drain(..).collect();

        let sequence = self.next_sequence;
        self.prune_local_state(sequence);

        let standard_txs: Vec<_> = transactions
            .iter()
            .enumerate()
            .filter_map(|(index, tx)| {
                parse_standard_transaction(tx).map(|(tx_id, state_key)| ParsedStandardTx {
                    tx_id,
                    state_key,
                    index,
                })
            })
            .collect();
        let batch_positions: HashMap<_, _> =
            standard_txs.iter().map(|tx| (tx.tx_id, tx.index)).collect();

        let mut edge_set = HashSet::new();
        let mut batch_writers: HashMap<u8, Vec<u64>> = HashMap::new();
        for tx in &standard_txs {
            if let Some(previous) = self.recent_local_txs.get(&tx.state_key) {
                for tracked in previous {
                    if tracked.tx_id != tx.tx_id {
                        edge_set.insert((tracked.tx_id, tx.tx_id));
                    }
                }
            }

            let prior_writers = batch_writers.entry(tx.state_key).or_default();
            for &prev_tx_id in prior_writers.iter() {
                if prev_tx_id != tx.tx_id {
                    edge_set.insert((prev_tx_id, tx.tx_id));
                }
            }

            if let Some(hints) = self.order_hints.get(&tx.tx_id) {
                for hint in hints {
                    if hint.state_key != tx.state_key || hint.predecessor == tx.tx_id {
                        continue;
                    }

                    if let Some(&predecessor_index) = batch_positions.get(&hint.predecessor) {
                        if predecessor_index >= tx.index {
                            continue;
                        }
                    } else {
                        edge_set.insert((hint.predecessor, tx.tx_id));
                    }
                }
            }

            if !prior_writers.contains(&tx.tx_id) {
                prior_writers.push(tx.tx_id);
            }
        }

        for tx in &standard_txs {
            self.track_local_tx(sequence, tx.state_key, tx.tx_id);
            self.order_hints.remove(&tx.tx_id);
        }
        self.prune_local_state(sequence);

        let mut edges: Vec<_> = edge_set.into_iter().collect();
        edges.sort_unstable();

        let batch = Batch {
            author: self.name,
            sequence,
            transactions,
            edges,
            missing_edges: Vec::new(),
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

    fn handle_control(&mut self, control: BatchMakerControl) {
        match control {
            BatchMakerControl::MergeOrderHints(hints) => {
                for hint in hints {
                    self.merge_order_hint(hint);
                }
            }
        }

        self.prune_local_state(self.next_sequence);
    }

    fn merge_order_hint(&mut self, hint: OrderHint) {
        if hint.weight == 0 {
            return;
        }

        let hints = self.order_hints.entry(hint.successor).or_default();
        if let Some(existing) = hints.iter_mut().find(|entry| {
            entry.predecessor == hint.predecessor && entry.state_key == hint.state_key
        }) {
            existing.weight = existing.weight.saturating_add(hint.weight);
            existing.observed_at_sequence =
                existing.observed_at_sequence.max(hint.observed_at_sequence);
        } else {
            hints.push(hint);
        }

        hints.sort_unstable_by(|left, right| {
            right
                .weight
                .cmp(&left.weight)
                .then_with(|| left.predecessor.cmp(&right.predecessor))
                .then_with(|| left.successor.cmp(&right.successor))
        });
        hints.truncate(Self::MAX_HINTS_PER_TX);
    }

    fn track_local_tx(&mut self, sequence: u64, state_key: u8, tx_id: u64) {
        let history = self.recent_local_txs.entry(state_key).or_default();
        if history.iter().any(|tracked| tracked.tx_id == tx_id) {
            return;
        }

        history.push_back(TrackedTx { tx_id, sequence });
        while history.len() > Self::MAX_RECENT_TXS_PER_KEY {
            history.pop_front();
        }
    }

    fn prune_local_state(&mut self, current_sequence: u64) {
        let min_local_sequence =
            current_sequence.saturating_sub(Self::MAX_LOCAL_HISTORY_SEQUENCES.saturating_sub(1));
        self.recent_local_txs.retain(|_, history| {
            while history
                .front()
                .map(|tracked| tracked.sequence < min_local_sequence)
                .unwrap_or(false)
            {
                history.pop_front();
            }
            while history.len() > Self::MAX_RECENT_TXS_PER_KEY {
                history.pop_front();
            }
            !history.is_empty()
        });

        let min_hint_sequence =
            current_sequence.saturating_sub(Self::MAX_HINT_HISTORY_SEQUENCES.saturating_sub(1));
        self.order_hints.retain(|_, hints| {
            hints.retain(|hint| hint.observed_at_sequence >= min_hint_sequence);
            hints.sort_unstable_by(|left, right| {
                right
                    .weight
                    .cmp(&left.weight)
                    .then_with(|| left.predecessor.cmp(&right.predecessor))
                    .then_with(|| left.successor.cmp(&right.successor))
            });
            hints.truncate(Self::MAX_HINTS_PER_TX);
            !hints.is_empty()
        });

        if self.order_hints.len() > Self::MAX_HINT_TARGETS {
            let remove_count = self.order_hints.len() - Self::MAX_HINT_TARGETS;
            let mut oldest_targets: Vec<_> = self
                .order_hints
                .iter()
                .map(|(&successor, hints)| {
                    let newest_sequence = hints
                        .iter()
                        .map(|hint| hint.observed_at_sequence)
                        .max()
                        .unwrap_or(0);
                    (successor, newest_sequence)
                })
                .collect();
            oldest_targets.sort_unstable_by_key(|(successor, newest_sequence)| {
                (*newest_sequence, *successor)
            });

            for (successor, _) in oldest_targets.into_iter().take(remove_count) {
                self.order_hints.remove(&successor);
            }
        }
    }
}
