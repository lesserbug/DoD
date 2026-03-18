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
use tokio::sync::mpsc::{channel, Receiver, Sender};
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CrossBatchDependency {
    edge_predecessor: Option<u64>,
    missing_predecessor: Option<u64>,
}

#[derive(Debug)]
struct PendingObservationControl {
    sequence: u64,
    missing_edges: VecDeque<(u64, u64)>,
}

#[derive(Debug)]
struct PendingProcessedControl {
    tx_ids: VecDeque<u64>,
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
    /// Best-effort observation channel fed by the worker-side executor.
    rx_observation_control: Receiver<BatchMakerControl>,
    /// Priority processed-feedback channel fed by the worker-side executor.
    rx_processed_control: Receiver<BatchMakerControl>,
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
    unprocessed_by_key: HashMap<u8, VecDeque<u64>>,
    /// Count of processed tx ids still lingering inside each per-key queue. We
    /// use this to trigger occasional local compaction without making every
    /// `MarkProcessed` update scan or rewrite the whole queue.
    stale_unprocessed_by_key: HashMap<u8, usize>,
    /// Per-key queue of unresolved local tx ids that currently have missing
    /// partners. This is the hot-path frontier index used when sealing the
    /// next local graph.
    frontier_by_key: HashMap<u8, VecDeque<u64>>,
    frontier_tx_ids: HashSet<u64>,
    /// Recent processed transaction ids retained long enough to preserve the
    /// local Processed/Unseen distinction without growing state unboundedly.
    processed_tx_ids: HashSet<u64>,
    processed_tx_fifo: VecDeque<u64>,
    /// Lightweight `M_w`: recent unresolved pairs touching locally known
    /// transactions, indexed by local tx id and bounded by age/size.
    missing_partners_by_tx: HashMap<u64, HashSet<u64>>,
    missing_pairs: HashSet<(u64, u64)>,
    missing_pair_fifo: VecDeque<(u64, (u64, u64))>,
    pending_observation_controls: VecDeque<PendingObservationControl>,
    pending_processed_controls: VecDeque<PendingProcessedControl>,
    /// Sequence number of the next local-order graph.
    next_sequence: u64,
}

impl BatchMaker {
    const INGRESS_CONTROL_WORK_BUDGET: usize = 1;
    const IDLE_CONTROL_WORK_BUDGET: usize = 4_096;
    const PRE_SEAL_CONTROL_WORK_BUDGET: usize = 8;
    const SEAL_CONTROL_WORK_BUDGET: usize = 0;
    const MAX_PROCESSED_TX_IDS: usize = 65_536;
    const MAX_MISSING_PAIRS: usize = 4_096;
    const MAX_MISSING_PAIR_AGE: u64 = 128;
    const COMPAT_CONTROL_CHANNEL_CAPACITY: usize = 1_024;

    pub fn spawn(
        name: PublicKey,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        rx_control: Receiver<BatchMakerControl>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        let (tx_observation_control, rx_observation_control) =
            channel(Self::COMPAT_CONTROL_CHANNEL_CAPACITY);
        let (tx_processed_control, rx_processed_control) =
            channel(Self::COMPAT_CONTROL_CHANNEL_CAPACITY);

        tokio::spawn(async move {
            let mut rx_control = rx_control;
            while let Some(control) = rx_control.recv().await {
                let result = match &control {
                    BatchMakerControl::ObserveGlobalGraph(_) => {
                        tx_observation_control.send(control).await
                    }
                    BatchMakerControl::MarkProcessed(_) => tx_processed_control.send(control).await,
                };

                if result.is_err() {
                    break;
                }
            }
        });

        Self::spawn_with_control_channels(
            name,
            batch_size,
            max_batch_delay,
            rx_transaction,
            rx_observation_control,
            rx_processed_control,
            tx_message,
            workers_addresses,
        );
    }

    pub fn spawn_with_control_channels(
        name: PublicKey,
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        rx_observation_control: Receiver<BatchMakerControl>,
        rx_processed_control: Receiver<BatchMakerControl>,
        tx_message: Sender<QuorumWaiterMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                batch_size,
                max_batch_delay,
                rx_transaction,
                rx_observation_control,
                rx_processed_control,
                tx_message,
                workers_addresses,
                current_batch: Vec::with_capacity(batch_size * 2),
                current_batch_standard_ids: HashSet::new(),
                current_batch_size: 0,
                network: ReliableSender::new(),
                last_writer: HashMap::new(),
                known_transactions: HashMap::new(),
                unprocessed_by_key: HashMap::new(),
                stale_unprocessed_by_key: HashMap::new(),
                frontier_by_key: HashMap::new(),
                frontier_tx_ids: HashSet::new(),
                processed_tx_ids: HashSet::new(),
                processed_tx_fifo: VecDeque::new(),
                missing_partners_by_tx: HashMap::new(),
                missing_pairs: HashSet::new(),
                missing_pair_fifo: VecDeque::new(),
                pending_observation_controls: VecDeque::new(),
                pending_processed_controls: VecDeque::new(),
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
                        self.drain_control_backlog(Self::PRE_SEAL_CONTROL_WORK_BUDGET);
                        self.seal().await;
                        0
                    } else {
                        Self::INGRESS_CONTROL_WORK_BUDGET
                    };

                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    budget
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    let budget = if !self.current_batch.is_empty() {
                        self.drain_control_backlog(Self::PRE_SEAL_CONTROL_WORK_BUDGET);
                        self.seal().await;
                        0
                    } else {
                        Self::IDLE_CONTROL_WORK_BUDGET
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
        self.drain_control_backlog(Self::SEAL_CONTROL_WORK_BUDGET);

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
        let mut cross_batch_dependencies = HashMap::new();
        for tx in &standard_txs {
            if !cross_batch_dependencies.contains_key(&tx.state_key) {
                let dependency = self.cross_batch_dependency_for_key(tx.state_key);
                cross_batch_dependencies.insert(tx.state_key, dependency);
            }
        }
        for tx in &standard_txs {
            if let Some(Some(prev_tx_id)) = cross_batch_dependencies
                .get(&tx.state_key)
                .map(|dependency| dependency.edge_predecessor)
            {
                if prev_tx_id != tx.tx_id {
                    edge_set.insert((prev_tx_id, tx.tx_id));
                }
            }

            if let Some(Some(frontier_tx_id)) = cross_batch_dependencies
                .get(&tx.state_key)
                .map(|dependency| dependency.missing_predecessor)
            {
                if frontier_tx_id != tx.tx_id && !edge_set.contains(&(frontier_tx_id, tx.tx_id)) {
                    missing_edge_set.insert((frontier_tx_id, tx.tx_id));
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

    fn drain_control_backlog(&mut self, work_budget: usize) {
        let mut remaining_work = work_budget;
        while remaining_work > 0 {
            if self.pending_processed_controls.is_empty()
                && self.pending_observation_controls.is_empty()
            {
                if !self.try_receive_control() {
                    break;
                }
            }

            if self.pending_processed_controls.is_empty() {
                self.try_receive_control();
            }
            if let Some(mut pending) = self.pending_processed_controls.pop_front() {
                let spent = self.process_pending_processed_control(&mut pending, remaining_work);
                if !pending.tx_ids.is_empty() {
                    self.pending_processed_controls.push_front(pending);
                }
                if spent == 0 {
                    break;
                }
                remaining_work = remaining_work.saturating_sub(spent);
                continue;
            }

            if self.pending_observation_controls.is_empty() {
                self.try_receive_control();
            }
            let Some(mut pending) = self.pending_observation_controls.pop_front() else {
                break;
            };
            let spent = self.process_pending_observation_control(&mut pending, remaining_work);
            if !pending.missing_edges.is_empty() {
                self.pending_observation_controls.push_front(pending);
            }
            if spent == 0 {
                break;
            }
            remaining_work = remaining_work.saturating_sub(spent);
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

    fn enqueue_control(&mut self, control: BatchMakerControl) {
        match control {
            BatchMakerControl::ObserveGlobalGraph(info) => {
                self.prune_missing_pairs(info.sequence);
                if !info.missing_edges.is_empty() {
                    self.pending_observation_controls
                        .push_back(PendingObservationControl {
                            sequence: info.sequence,
                            missing_edges: info.missing_edges.into(),
                        });
                }
            }
            BatchMakerControl::MarkProcessed(tx_ids) => {
                if !tx_ids.is_empty() {
                    self.pending_processed_controls
                        .push_back(PendingProcessedControl {
                            tx_ids: tx_ids.into(),
                        });
                }
            }
        }
    }

    fn process_pending_observation_control(
        &mut self,
        pending: &mut PendingObservationControl,
        work_budget: usize,
    ) -> usize {
        let mut spent = 0;
        while spent < work_budget {
            let Some(pair) = pending.missing_edges.pop_front() else {
                break;
            };
            self.handle_observed_missing_pair(pending.sequence, pair);
            spent += 1;
        }
        spent
    }

    fn process_pending_processed_control(
        &mut self,
        pending: &mut PendingProcessedControl,
        work_budget: usize,
    ) -> usize {
        let mut spent = 0;
        let mut touched_keys = HashSet::new();
        while spent < work_budget {
            let Some(tx_id) = pending.tx_ids.pop_front() else {
                break;
            };
            self.mark_processed_one(tx_id, &mut touched_keys);
            spent += 1;
        }
        self.prune_empty_unprocessed_keys(touched_keys);
        spent
    }

    fn try_receive_control(&mut self) -> bool {
        if let Ok(control) = self.rx_processed_control.try_recv() {
            self.enqueue_control(control);
            return true;
        }

        if let Ok(control) = self.rx_observation_control.try_recv() {
            self.enqueue_control(control);
            return true;
        }

        false
    }

    fn record_unprocessed(&mut self, tx_id: u64, state_key: u8) {
        if self.known_transactions.insert(tx_id, state_key).is_none() {
            self.unprocessed_by_key
                .entry(state_key)
                .or_default()
                .push_back(tx_id);
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
            self.handle_observed_missing_pair(info.sequence, pair);
        }
    }

    fn handle_observed_missing_pair(&mut self, sequence: u64, pair: (u64, u64)) {
        let (left, right) = pair;
        let left_state = self.tx_state(left);
        let right_state = self.tx_state(right);
        if left_state == TxState::Processed || right_state == TxState::Processed {
            return;
        }

        if left_state != TxState::Unprocessed && right_state != TxState::Unprocessed {
            return;
        }

        self.insert_missing_pair(sequence, (left, right));
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
        if partners.insert(partner) {
            self.enqueue_frontier_tx(tx_id);
        }
    }

    fn has_missing_partners(&self, tx_id: u64) -> bool {
        self.missing_partners_by_tx
            .get(&tx_id)
            .map_or(false, |partners| !partners.is_empty())
    }

    fn unresolved_frontier_for_key(&mut self, state_key: u8) -> Option<u64> {
        self.prune_frontier_queue(state_key);
        self.frontier_by_key
            .get(&state_key)
            .and_then(|tx_ids| tx_ids.front().copied())
    }

    fn cross_batch_dependency_for_key(&mut self, state_key: u8) -> CrossBatchDependency {
        let unresolved_frontier = self.unresolved_frontier_for_key(state_key);
        let Some(last_writer) = self.last_writer.get(&state_key).copied() else {
            return CrossBatchDependency {
                edge_predecessor: None,
                missing_predecessor: unresolved_frontier,
            };
        };

        if self.tx_state(last_writer) == TxState::Unprocessed && unresolved_frontier.is_some() {
            // If any earlier local tx on this key is still unresolved, the
            // current local last-writer remains tainted by that unresolved
            // chain and should be carried as a missing predecessor rather than
            // as a stable cross-batch edge.
            CrossBatchDependency {
                edge_predecessor: None,
                missing_predecessor: Some(last_writer),
            }
        } else {
            CrossBatchDependency {
                edge_predecessor: Some(last_writer),
                missing_predecessor: unresolved_frontier
                    .filter(|frontier| *frontier != last_writer),
            }
        }
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
                partners.remove(&partner);
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
            self.mark_processed_one(tx_id, &mut touched_keys);
        }

        self.prune_empty_unprocessed_keys(touched_keys);
    }

    fn mark_processed_one(&mut self, tx_id: u64, touched_keys: &mut HashSet<u8>) {
        self.remember_processed(tx_id);
        if let Some(state_key) = self.known_transactions.remove(&tx_id) {
            touched_keys.insert(state_key);
            *self.stale_unprocessed_by_key.entry(state_key).or_insert(0) += 1;
        }
        self.remove_all_missing_pairs_for(tx_id);
    }

    fn prune_empty_unprocessed_keys(&mut self, touched_keys: HashSet<u8>) {
        for state_key in touched_keys {
            self.prune_unprocessed_prefix(state_key);
            let should_remove = self
                .unprocessed_by_key
                .get(&state_key)
                .map_or(false, VecDeque::is_empty);
            if should_remove {
                self.unprocessed_by_key.remove(&state_key);
            }
        }
    }

    fn enqueue_frontier_tx(&mut self, tx_id: u64) {
        let Some(&state_key) = self.known_transactions.get(&tx_id) else {
            return;
        };

        if self.frontier_tx_ids.insert(tx_id) {
            self.frontier_by_key
                .entry(state_key)
                .or_default()
                .push_back(tx_id);
        }
    }

    fn prune_frontier_queue(&mut self, state_key: u8) {
        loop {
            let tx_id = match self
                .frontier_by_key
                .get(&state_key)
                .and_then(|queue| queue.front().copied())
            {
                Some(tx_id) => tx_id,
                None => break,
            };

            if self.known_transactions.contains_key(&tx_id) && self.has_missing_partners(tx_id) {
                break;
            }

            if let Some(queue) = self.frontier_by_key.get_mut(&state_key) {
                queue.pop_front();
            }
            self.frontier_tx_ids.remove(&tx_id);
        }

        let should_remove = self
            .frontier_by_key
            .get(&state_key)
            .map_or(false, VecDeque::is_empty);
        if should_remove {
            self.frontier_by_key.remove(&state_key);
        }
    }

    fn prune_unprocessed_prefix(&mut self, state_key: u8) {
        let known_transactions = &self.known_transactions;
        let mut removed = 0usize;
        let should_remove = match self.unprocessed_by_key.get_mut(&state_key) {
            Some(unprocessed) => {
                while let Some(&tx_id) = unprocessed.front() {
                    if known_transactions.contains_key(&tx_id) {
                        break;
                    }
                    unprocessed.pop_front();
                    removed += 1;
                }
                unprocessed.is_empty()
            }
            None => false,
        };

        if removed > 0 {
            self.reduce_stale_unprocessed_count(state_key, removed);
        }

        if should_remove {
            self.unprocessed_by_key.remove(&state_key);
            self.stale_unprocessed_by_key.remove(&state_key);
        }
    }

    fn reduce_stale_unprocessed_count(&mut self, state_key: u8, removed: usize) {
        let Some(stale) = self.stale_unprocessed_by_key.get_mut(&state_key) else {
            return;
        };

        *stale = stale.saturating_sub(removed);
        if *stale == 0 {
            self.stale_unprocessed_by_key.remove(&state_key);
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
        let Some(partners) = self.missing_partners_by_tx.remove(&tx_id) else {
            return;
        };

        for partner in partners {
            let pair = canonical_missing_edge(tx_id, partner);
            self.remove_missing_pair(pair);
        }
    }
}
