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
    // Within one local graph we keep every earlier conflicting transaction,
    // while across graphs we only carry the latest known predecessor.
    pub edges: Vec<(u64, u64)>,
    // Missing dependency pairs carried without re-broadcasting old payloads.
    // Pairs emitted by the local worker keep the historical predecessor first;
    // pairs synthesized by global ordering remain unresolved same-round pairs.
    #[serde(default)]
    pub missing_edges: Vec<(u64, u64)>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalGraphInfo {
    pub sequence: u64,
    pub tx_ids: Vec<u64>,
    pub missing_edges: Vec<(u64, u64)>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchMakerControl {
    ObserveGlobalGraph(GlobalGraphInfo),
    MarkProcessed(Vec<u64>),
}

#[allow(dead_code)]
impl BatchMakerControl {
    pub fn observe_global_batch(batch: &Batch) -> Self {
        let mut tx_ids: Vec<_> = batch
            .transactions
            .iter()
            .filter_map(|tx| parse_standard_transaction(tx).map(|(tx_id, _)| tx_id))
            .collect();
        tx_ids.sort_unstable();
        tx_ids.dedup();

        let mut missing_edges: Vec<_> = batch
            .missing_edges
            .iter()
            .map(|&(left, right)| canonical_missing_edge(left, right))
            .collect();
        missing_edges.sort_unstable();
        missing_edges.dedup();

        Self::ObserveGlobalGraph(GlobalGraphInfo {
            sequence: batch.sequence,
            tx_ids,
            missing_edges,
        })
    }

    pub fn mark_processed(mut tx_ids: Vec<u64>) -> Self {
        tx_ids.sort_unstable();
        tx_ids.dedup();
        Self::MarkProcessed(tx_ids)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ParsedStandardTx {
    tx_id: u64,
    state_key: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxState {
    Processed,
    Unprocessed,
    InBatch,
    Unseen,
}

#[allow(dead_code)]
fn canonical_missing_edge(left: u64, right: u64) -> (u64, u64) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
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
    /// Channel to receive execution feedback from the worker-side executor.
    rx_control: Receiver<BatchMakerControl>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch of fresh client transactions.
    current_batch: Vec<Transaction>,
    /// Standard transaction ids currently staged in the open local graph.
    current_batch_standard_ids: HashSet<u64>,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    /// Records the latest writer transaction id for each key across sealed
    /// local graphs. This is the stable cross-batch predecessor evidence used
    /// by the current DoD worker pipeline.
    last_writer: HashMap<u8, u64>,
    /// Lightweight local `S` skeleton: tx ids we have sealed locally but that
    /// have not yet been reported as executed back by the worker-side executor.
    known_transactions: HashMap<u64, u8>,
    /// Recent unprocessed transactions grouped by state key. We keep only tx ids
    /// here so the Processed feedback loop does not retain payloads in the hot
    /// path.
    unprocessed_by_key: HashMap<u8, Vec<u64>>,
    /// Recent processed transaction ids retained long enough to preserve the
    /// local Processed/Unseen distinction without growing state unboundedly.
    processed_tx_ids: HashSet<u64>,
    processed_tx_fifo: VecDeque<u64>,
    /// Lightweight `M_w`: recent unresolved pairs touching locally known
    /// transactions, indexed by local tx id and bounded by age/size.
    missing_partners_by_tx: HashMap<u64, Vec<u64>>,
    missing_pairs: HashSet<(u64, u64)>,
    missing_pair_fifo: VecDeque<(u64, (u64, u64))>,
    /// Sequence number of the next local-order graph.
    next_sequence: u64,
}

impl BatchMaker {
    const INGRESS_CONTROL_BUDGET: usize = 1;
    const IDLE_CONTROL_BUDGET: usize = 64;
    const SEAL_CONTROL_BUDGET: usize = 64;
    const MAX_PROCESSED_TX_IDS: usize = 65_536;
    const MAX_MISSING_PAIRS: usize = 4_096;
    const MAX_MISSING_PAIR_AGE: u64 = 128;

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
                current_batch_standard_ids: HashSet::new(),
                current_batch_size: 0,
                network: ReliableSender::new(),
                last_writer: HashMap::new(),
                known_transactions: HashMap::new(),
                unprocessed_by_key: HashMap::new(),
                processed_tx_ids: HashSet::new(),
                processed_tx_fifo: VecDeque::new(),
                missing_partners_by_tx: HashMap::new(),
                missing_pairs: HashSet::new(),
                missing_pair_fifo: VecDeque::new(),
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
            let control_budget = tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    let budget = if self.accept_transaction(transaction)
                        && self.current_batch_size >= self.batch_size
                    {
                        self.seal().await;
                        0
                    } else {
                        Self::INGRESS_CONTROL_BUDGET
                    };

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    budget
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    let budget = if !self.current_batch.is_empty() {
                        self.seal().await;
                        0
                    } else {
                        Self::IDLE_CONTROL_BUDGET
                    };

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    budget
                }
            };

            if control_budget > 0 {
                self.drain_control_backlog(control_budget);
            }

            // Give the chance to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        self.drain_control_backlog(Self::SEAL_CONTROL_BUDGET);

        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx.first() == Some(&0u8) && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        self.current_batch_size = 0;
        self.current_batch_standard_ids.clear();
        let transactions: Vec<_> = self.current_batch.drain(..).collect();
        let sequence = self.next_sequence;

        let standard_txs: Vec<_> = transactions
            .iter()
            .filter_map(|tx| {
                parse_standard_transaction(tx)
                    .map(|(tx_id, state_key)| ParsedStandardTx { tx_id, state_key })
            })
            .collect();

        let mut edge_set = HashSet::new();
        let mut missing_edge_set = HashSet::new();
        let mut batch_writers: HashMap<u8, Vec<u64>> = HashMap::new();
        let mut unresolved_frontiers = HashMap::new();
        for tx in &standard_txs {
            unresolved_frontiers
                .entry(tx.state_key)
                .or_insert_with(|| self.unresolved_frontier_for_key(tx.state_key));
        }
        for tx in &standard_txs {
            if let Some(&prev_tx_id) = self.last_writer.get(&tx.state_key) {
                if prev_tx_id != tx.tx_id {
                    edge_set.insert((prev_tx_id, tx.tx_id));
                }
            }

            if let Some(Some(frontier_tx_id)) = unresolved_frontiers.get(&tx.state_key) {
                if *frontier_tx_id != tx.tx_id && !edge_set.contains(&(*frontier_tx_id, tx.tx_id)) {
                    missing_edge_set.insert((*frontier_tx_id, tx.tx_id));
                }
            }

            let prior_writers = batch_writers.entry(tx.state_key).or_default();
            for &prev_tx_id in prior_writers.iter() {
                if prev_tx_id != tx.tx_id {
                    edge_set.insert((prev_tx_id, tx.tx_id));
                }
            }
            if !prior_writers.contains(&tx.tx_id) {
                prior_writers.push(tx.tx_id);
            }
        }

        for (state_key, writers) in batch_writers {
            if let Some(&latest_tx_id) = writers.last() {
                self.last_writer.insert(state_key, latest_tx_id);
            }
        }

        for tx in &standard_txs {
            self.record_unprocessed(tx.tx_id, tx.state_key);
        }

        let mut edges: Vec<_> = edge_set.into_iter().collect();
        edges.sort_unstable();
        let mut missing_edges: Vec<_> = missing_edge_set.into_iter().collect();
        missing_edges.sort_unstable();

        let batch = Batch {
            author: self.name,
            sequence,
            transactions,
            edges,
            missing_edges,
        };
        self.next_sequence += 1;

        let message = WorkerMessage::LocalBatch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                info!(
                    "LocalGraph {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            info!("LocalGraph {:?} contains {} B", digest, size);
        }

        let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        self.tx_message
            .send(QuorumWaiterMessage {
                batch: serialized,
                handlers: names.into_iter().zip(handlers.into_iter()).collect(),
            })
            .await
            .expect("Failed to deliver batch");
    }

    fn drain_control_backlog(&mut self, budget: usize) {
        for _ in 0..budget {
            let Ok(control) = self.rx_control.try_recv() else {
                break;
            };
            self.handle_control(control);
        }
    }

    fn accept_transaction(&mut self, transaction: Transaction) -> bool {
        if let Some((tx_id, _)) = parse_standard_transaction(&transaction) {
            // Keep the local state machine explicit, but avoid paying the
            // Processed-set lookup on every fresh transaction. Until the full
            // Algorithm 1 state machine is in place, hot-path duplicate
            // filtering only blocks txs already staged in the open batch or
            // still locally Unprocessed.
            if self.current_batch_standard_ids.contains(&tx_id)
                || self.known_transactions.contains_key(&tx_id)
            {
                return false;
            }

            self.current_batch_standard_ids.insert(tx_id);
        }

        self.current_batch_size += transaction.len();
        self.current_batch.push(transaction);
        true
    }

    fn handle_control(&mut self, control: BatchMakerControl) {
        match control {
            BatchMakerControl::ObserveGlobalGraph(info) => self.observe_global_graph(info),
            BatchMakerControl::MarkProcessed(tx_ids) => self.mark_processed(tx_ids),
        }
    }

    fn record_unprocessed(&mut self, tx_id: u64, state_key: u8) {
        if self.known_transactions.insert(tx_id, state_key).is_none() {
            self.unprocessed_by_key
                .entry(state_key)
                .or_default()
                .push(tx_id);
        }
    }

    fn tx_state(&self, tx_id: u64) -> TxState {
        if self.processed_tx_ids.contains(&tx_id) {
            TxState::Processed
        } else if self.known_transactions.contains_key(&tx_id) {
            TxState::Unprocessed
        } else if self.current_batch_standard_ids.contains(&tx_id) {
            TxState::InBatch
        } else {
            TxState::Unseen
        }
    }

    fn observe_global_graph(&mut self, info: GlobalGraphInfo) {
        self.prune_missing_pairs(info.sequence);

        for pair in info.missing_edges {
            let (left, right) = pair;
            let left_state = self.tx_state(left);
            let right_state = self.tx_state(right);
            if left_state == TxState::Processed || right_state == TxState::Processed {
                continue;
            }

            if left_state != TxState::Unprocessed && right_state != TxState::Unprocessed {
                continue;
            }

            self.insert_missing_pair(info.sequence, (left, right));
        }
    }

    fn insert_missing_pair(&mut self, sequence: u64, pair: (u64, u64)) {
        if pair.0 == pair.1 || !self.missing_pairs.insert(pair) {
            return;
        }

        self.missing_pair_fifo.push_back((sequence, pair));
        self.record_missing_partner(pair.0, pair.1);
        self.record_missing_partner(pair.1, pair.0);
        self.prune_missing_pairs(sequence);
    }

    fn record_missing_partner(&mut self, tx_id: u64, partner: u64) {
        if self.tx_state(tx_id) != TxState::Unprocessed {
            return;
        }

        let partners = self.missing_partners_by_tx.entry(tx_id).or_default();
        if !partners.contains(&partner) {
            partners.push(partner);
        }
    }

    fn has_missing_partners(&self, tx_id: u64) -> bool {
        self.missing_partners_by_tx
            .get(&tx_id)
            .map_or(false, |partners| !partners.is_empty())
    }

    fn unresolved_frontier_for_key(&self, state_key: u8) -> Option<u64> {
        self.unprocessed_by_key.get(&state_key).and_then(|tx_ids| {
            tx_ids.iter().copied().find(|tx_id| {
                self.tx_state(*tx_id) == TxState::Unprocessed && self.has_missing_partners(*tx_id)
            })
        })
    }

    fn prune_missing_pairs(&mut self, current_sequence: u64) {
        loop {
            let Some(&(sequence, pair)) = self.missing_pair_fifo.front() else {
                break;
            };

            let too_many = self.missing_pairs.len() > Self::MAX_MISSING_PAIRS;
            let too_old = current_sequence.saturating_sub(sequence) > Self::MAX_MISSING_PAIR_AGE;
            if !too_many && !too_old {
                break;
            }

            self.missing_pair_fifo.pop_front();
            self.remove_missing_pair(pair);
        }
    }

    fn remove_missing_pair(&mut self, pair: (u64, u64)) {
        if !self.missing_pairs.remove(&pair) {
            return;
        }

        self.remove_missing_partner(pair.0, pair.1);
        self.remove_missing_partner(pair.1, pair.0);
    }

    fn remove_missing_partner(&mut self, tx_id: u64, partner: u64) {
        let should_remove = match self.missing_partners_by_tx.get_mut(&tx_id) {
            Some(partners) => {
                partners.retain(|candidate| *candidate != partner);
                partners.is_empty()
            }
            None => false,
        };

        if should_remove {
            self.missing_partners_by_tx.remove(&tx_id);
        }
    }

    fn mark_processed(&mut self, tx_ids: Vec<u64>) {
        let mut touched_keys = HashSet::new();

        for tx_id in tx_ids {
            self.remember_processed(tx_id);
            if let Some(state_key) = self.known_transactions.remove(&tx_id) {
                touched_keys.insert(state_key);
                if let Some(unprocessed) = self.unprocessed_by_key.get_mut(&state_key) {
                    unprocessed.retain(|candidate| *candidate != tx_id);
                }
            }
            self.remove_all_missing_pairs_for(tx_id);
        }

        for state_key in touched_keys {
            let should_remove = self
                .unprocessed_by_key
                .get(&state_key)
                .map_or(false, Vec::is_empty);
            if should_remove {
                self.unprocessed_by_key.remove(&state_key);
            }
        }
    }

    fn remember_processed(&mut self, tx_id: u64) {
        if !self.processed_tx_ids.insert(tx_id) {
            return;
        }

        self.processed_tx_fifo.push_back(tx_id);
        while self.processed_tx_fifo.len() > Self::MAX_PROCESSED_TX_IDS {
            let Some(evicted) = self.processed_tx_fifo.pop_front() else {
                break;
            };
            self.processed_tx_ids.remove(&evicted);
        }
    }

    fn remove_all_missing_pairs_for(&mut self, tx_id: u64) {
        let Some(partners) = self.missing_partners_by_tx.get(&tx_id).cloned() else {
            return;
        };

        for partner in partners {
            let pair = canonical_missing_edge(tx_id, partner);
            self.remove_missing_pair(pair);
        }

        self.missing_partners_by_tx.remove(&tx_id);
    }
}
