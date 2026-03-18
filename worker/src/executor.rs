// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{
    parse_standard_transaction, parse_transaction_id_and_state_key, Batch, BatchMakerControl,
    GlobalGraphInfo, Transaction,
};
use crate::worker::WorkerMessage;
use config::WorkerId;
use crypto::Digest;
#[cfg(feature = "benchmark")]
use log::info;
use log::warn;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{error::TrySendError, Receiver, Sender};
use tokio::time::{interval, Duration};

#[cfg(test)]
#[path = "tests/executor_tests.rs"]
pub mod executor_tests;

struct PendingBatch {
    digest: Digest,
    batch: Batch,
    external_dependencies: Vec<u64>,
    same_batch_pair_count: usize,
    first_seen_sequence: u64,
    stalled_rounds: u32,
}

#[derive(Default)]
struct MissingEdgeSummary {
    external_dependencies: Vec<u64>,
    same_batch_pair_count: usize,
}

/// Executes globally ordered batches once the primary feeds back their ordered
/// digests. This is the worker-side skeleton of DoD Algorithm 3.
pub struct Executor {
    /// Our worker id.
    id: WorkerId,
    /// Persistent batch store shared with the processor.
    store: Store,
    /// Ordered digests to execute, as fed back by the primary.
    rx_execute: Receiver<Vec<(Digest, WorkerId)>>,
    /// Best-effort global-graph observations sent back to the local batch
    /// maker.
    tx_batch_observation: Sender<BatchMakerControl>,
    /// Priority processed tx feedback sent back to the local batch maker.
    tx_batch_processed: Sender<BatchMakerControl>,
    /// Avoid re-executing the same committed digest.
    executed: HashSet<Digest>,
    /// Recent processed tx ids, kept long enough to release queued batches with
    /// cross-batch missing predecessors.
    processed_tx_ids: HashSet<u64>,
    processed_fifo: VecDeque<u64>,
    /// Ordered global batches waiting for external missing predecessors to be
    /// marked as processed locally.
    pending_batches: VecDeque<PendingBatch>,
    /// Reference counts for processed predecessors still needed by queued batches.
    pending_dependency_counts: HashMap<u64, usize>,
    /// Latest globally ordered sequence observed by this executor.
    latest_sequence: u64,
    /// Best-effort observations temporarily buffered while the batch-maker
    /// observation channel is saturated. This smooths bursts without blocking
    /// the execution path or the processed-feedback path.
    pending_observations: VecDeque<GlobalGraphInfo>,
    /// Number of dropped observe-global-graph control messages.
    dropped_observations: u64,
    /// Number of soft-limit queue health events emitted.
    pending_health_events: u64,
    /// Number of times processed-id eviction was blocked by pending dependencies.
    processed_trim_blocked_events: u64,
    /// Number of batches released with same-batch ambiguous pairs resolved by
    /// the deterministic fallback.
    same_batch_fallback_batches: u64,
    /// Total count of same-batch ambiguous pairs resolved by the deterministic
    /// fallback.
    same_batch_fallback_pairs: u64,
    /// Whether to emit benchmark execution logs.
    benchmark_log_batches: bool,
}

impl Executor {
    /// The maximum number of processed tx ids retained for resolving
    /// carry-over missing predecessors.
    const MAX_PROCESSED_TX_IDS: usize = 65_536;
    const MAX_PENDING_BATCHES_SOFT: usize = 4_096;
    const MAX_PENDING_SEQUENCE_LAG_SOFT: u64 = 256;
    const MAX_PENDING_OBSERVATIONS: usize = 1_024;
    const OBSERVATION_FLUSH_BUDGET: usize = 16;
    const OBSERVATION_IDLE_FLUSH_BUDGET: usize = 256;
    const HEALTH_LOG_INTERVAL: u64 = 64;
    const BENCHMARK_HEALTH_LOG_INTERVAL: u64 = 1_024;
    const METRICS_REPORT_INTERVAL_SECS: u64 = 5;

    pub fn spawn(
        id: WorkerId,
        store: Store,
        rx_execute: Receiver<Vec<(Digest, WorkerId)>>,
        tx_batch_observation: Sender<BatchMakerControl>,
        tx_batch_processed: Sender<BatchMakerControl>,
        benchmark_log_batches: bool,
    ) {
        tokio::spawn(async move {
            Self {
                id,
                store,
                rx_execute,
                tx_batch_observation,
                tx_batch_processed,
                executed: HashSet::new(),
                processed_tx_ids: HashSet::new(),
                processed_fifo: VecDeque::new(),
                pending_batches: VecDeque::new(),
                pending_dependency_counts: HashMap::new(),
                latest_sequence: 0,
                pending_observations: VecDeque::new(),
                dropped_observations: 0,
                pending_health_events: 0,
                processed_trim_blocked_events: 0,
                same_batch_fallback_batches: 0,
                same_batch_fallback_pairs: 0,
                benchmark_log_batches,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut metrics_tick = interval(Duration::from_secs(Self::METRICS_REPORT_INTERVAL_SECS));

        loop {
            tokio::select! {
                message = self.rx_execute.recv() => {
                    let Some(ordered_batches) = message else {
                        self.flush_observation_backlog(Self::OBSERVATION_IDLE_FLUSH_BUDGET);
                        self.log_metrics_snapshot();
                        break;
                    };
                    for (digest, worker_id) in ordered_batches {
                        if worker_id != self.id || !self.executed.insert(digest.clone()) {
                            continue;
                        }

                        self.flush_observation_backlog(Self::OBSERVATION_FLUSH_BUDGET);

                        let serialized = match self.store.notify_read(digest.to_vec()).await {
                            Ok(bytes) => bytes,
                            Err(error) => {
                                warn!(
                                    "Executor failed to read ordered batch {}: {}",
                                    digest, error
                                );
                                continue;
                            }
                        };

                        let batch = match bincode::deserialize::<WorkerMessage>(&serialized) {
                            Ok(WorkerMessage::GlobalBatch(batch)) => batch,
                            Ok(other) => {
                                warn!(
                                    "Executor received unexpected stored worker message: {:?}",
                                    other
                                );
                                continue;
                            }
                            Err(error) => {
                                warn!(
                                    "Executor failed to deserialize ordered batch {}: {}",
                                    digest, error
                                );
                                continue;
                            }
                        };
                        self.latest_sequence = self.latest_sequence.max(batch.sequence);
                        let missing_edge_summary = Self::summarize_missing_edges(&batch);

                        self.observe_global_batch_best_effort(&batch);

                        if Self::batch_ready(
                            &missing_edge_summary.external_dependencies,
                            &self.processed_tx_ids,
                        ) {
                            self.execute_and_feedback(
                                digest,
                                batch,
                                missing_edge_summary.same_batch_pair_count,
                            )
                            .await;
                            self.retry_pending_batches().await;
                        } else {
                            self.enqueue_pending_batch(digest, batch, missing_edge_summary);
                        }
                    }
                }
                _ = metrics_tick.tick() => {
                    self.flush_observation_backlog(Self::OBSERVATION_IDLE_FLUSH_BUDGET);
                    self.log_metrics_snapshot();
                }
            }
        }
    }

    fn observe_global_batch_best_effort(&mut self, batch: &Batch) {
        if batch.missing_edges.is_empty() {
            return;
        }

        match self
            .tx_batch_observation
            .try_send(BatchMakerControl::observe_global_batch(batch))
        {
            Ok(()) => {}
            Err(TrySendError::Full(BatchMakerControl::ObserveGlobalGraph(info))) => {
                self.buffer_observation(info);
            }
            Err(TrySendError::Closed(BatchMakerControl::ObserveGlobalGraph(_))) => {
                self.record_observation_drop();
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                unreachable!("executor observation channel only carries ObserveGlobalGraph");
            }
        }
    }

    fn flush_observation_backlog(&mut self, budget: usize) {
        let mut sent = 0usize;
        while sent < budget {
            let Some(info) = self.pending_observations.pop_front() else {
                break;
            };

            match self
                .tx_batch_observation
                .try_send(BatchMakerControl::ObserveGlobalGraph(info))
            {
                Ok(()) => {
                    sent += 1;
                }
                Err(TrySendError::Full(BatchMakerControl::ObserveGlobalGraph(info))) => {
                    self.pending_observations.push_front(info);
                    break;
                }
                Err(TrySendError::Closed(BatchMakerControl::ObserveGlobalGraph(_))) => {
                    self.record_observation_drop();
                    self.pending_observations.clear();
                    break;
                }
                Err(TrySendError::Full(_)) | Err(TrySendError::Closed(_)) => {
                    unreachable!("executor observation channel only carries ObserveGlobalGraph");
                }
            }
        }
    }

    fn buffer_observation(&mut self, info: GlobalGraphInfo) {
        if self.pending_observations.len() >= Self::MAX_PENDING_OBSERVATIONS {
            self.pending_observations.pop_front();
            self.record_observation_drop();
        }

        self.pending_observations.push_back(info);
    }

    fn record_observation_drop(&mut self) {
        self.dropped_observations += 1;
        if self.should_log_event(self.dropped_observations) {
            warn!(
                "Executor dropped {} global-graph observations while updating local M_w",
                self.dropped_observations
            );
        }
    }

    fn batch_ready(external_dependencies: &[u64], processed_tx_ids: &HashSet<u64>) -> bool {
        external_dependencies
            .iter()
            .all(|dependency| processed_tx_ids.contains(dependency))
    }

    fn execute_batch(batch: &Batch) -> Vec<Transaction> {
        let node_count = batch.transactions.len();
        if node_count <= 1 {
            return batch.transactions.clone();
        }

        let positions: HashMap<u64, usize> = batch
            .transactions
            .iter()
            .enumerate()
            .filter_map(|(index, tx)| {
                parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| (tx_id, index))
            })
            .collect();

        let mut adjacency: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        let mut indegree = vec![0usize; node_count];

        let mut add_edge = |from: usize, to: usize| {
            if from == to {
                return;
            }
            if adjacency.entry(from).or_default().insert(to) {
                indegree[to] += 1;
            }
        };

        for &(from, to) in &batch.edges {
            if let (Some(&from_index), Some(&to_index)) = (positions.get(&from), positions.get(&to))
            {
                add_edge(from_index, to_index);
            }
        }

        // Cross-batch missing predecessors are handled by the local execution
        // queue before we reach this point. For same-batch ambiguous pairs we
        // still keep the finalized batch order as a deterministic tie-breaker.
        for &(left, right) in &batch.missing_edges {
            if let (Some(&left_index), Some(&right_index)) =
                (positions.get(&left), positions.get(&right))
            {
                if left_index < right_index {
                    add_edge(left_index, right_index);
                } else if right_index < left_index {
                    add_edge(right_index, left_index);
                }
            }
        }

        let mut ready = BTreeSet::new();
        for (index, degree) in indegree.iter().enumerate() {
            if *degree == 0 {
                ready.insert(index);
            }
        }

        let mut ordered = Vec::with_capacity(node_count);
        while let Some(index) = ready.iter().next().copied() {
            ready.remove(&index);
            ordered.push(batch.transactions[index].clone());

            if let Some(neighbors) = adjacency.get(&index) {
                for &next in neighbors {
                    indegree[next] -= 1;
                    if indegree[next] == 0 {
                        ready.insert(next);
                    }
                }
            }
        }

        if ordered.len() < node_count {
            let remaining: HashSet<_> = ordered
                .iter()
                .filter_map(|tx| parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| tx_id))
                .collect();
            for tx in &batch.transactions {
                match parse_transaction_id_and_state_key(tx) {
                    Some((tx_id, _)) if remaining.contains(&tx_id) => {}
                    _ => ordered.push(tx.clone()),
                }
            }
        }

        ordered
    }

    async fn execute_and_feedback(
        &mut self,
        digest: Digest,
        batch: Batch,
        same_batch_pair_count: usize,
    ) {
        self.record_same_batch_fallback(same_batch_pair_count, batch.sequence, &digest);
        let executed_transactions = Self::execute_batch(&batch);
        let processed_tx_ids = Self::collect_processed_tx_ids(&executed_transactions);
        self.remember_processed(&processed_tx_ids);

        if !processed_tx_ids.is_empty() {
            self.tx_batch_processed
                .send(BatchMakerControl::mark_processed(processed_tx_ids))
                .await
                .expect("Failed to send processed feedback to batch maker");
        }

        #[cfg(not(feature = "benchmark"))]
        let _ = (&executed_transactions, self.benchmark_log_batches, digest);
        #[cfg(feature = "benchmark")]
        if self.benchmark_log_batches {
            Self::log_executed_batch(&digest, &executed_transactions);
        }
    }

    async fn retry_pending_batches(&mut self) {
        loop {
            let pending_len = self.pending_batches.len();
            if pending_len == 0 {
                break;
            }

            let mut progressed = false;
            for _ in 0..pending_len {
                let pending = self
                    .pending_batches
                    .pop_front()
                    .expect("pending batch queue length changed unexpectedly");

                if Self::batch_ready(&pending.external_dependencies, &self.processed_tx_ids) {
                    self.untrack_pending_dependencies(&pending.external_dependencies);
                    self.execute_and_feedback(
                        pending.digest,
                        pending.batch,
                        pending.same_batch_pair_count,
                    )
                    .await;
                    progressed = true;
                } else {
                    let mut pending = pending;
                    pending.stalled_rounds = pending.stalled_rounds.saturating_add(1);
                    self.pending_batches.push_back(pending);
                }
            }

            if !progressed {
                self.maybe_log_pending_queue_health();
                break;
            }
        }
    }

    fn remember_processed(&mut self, tx_ids: &[u64]) {
        for &tx_id in tx_ids {
            if self.processed_tx_ids.insert(tx_id) {
                self.processed_fifo.push_back(tx_id);
            }
        }

        self.trim_processed_to_cap(Self::MAX_PROCESSED_TX_IDS);
    }

    fn collect_processed_tx_ids(transactions: &[Transaction]) -> Vec<u64> {
        let mut tx_ids: Vec<_> = transactions
            .iter()
            .filter_map(|tx| parse_standard_transaction(tx).map(|(tx_id, _)| tx_id))
            .collect();
        tx_ids.sort_unstable();
        tx_ids.dedup();
        tx_ids
    }

    fn enqueue_pending_batch(
        &mut self,
        digest: Digest,
        batch: Batch,
        missing_edge_summary: MissingEdgeSummary,
    ) {
        self.track_pending_dependencies(&missing_edge_summary.external_dependencies);
        self.pending_batches.push_back(PendingBatch {
            digest,
            batch,
            external_dependencies: missing_edge_summary.external_dependencies,
            same_batch_pair_count: missing_edge_summary.same_batch_pair_count,
            first_seen_sequence: self.latest_sequence,
            stalled_rounds: 0,
        });
        self.maybe_log_pending_queue_health();
    }

    fn summarize_missing_edges(batch: &Batch) -> MissingEdgeSummary {
        if batch.missing_edges.is_empty() {
            return MissingEdgeSummary::default();
        }

        let positions: HashSet<_> = batch
            .transactions
            .iter()
            .filter_map(|tx| parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| tx_id))
            .collect();

        let mut external_dependencies = Vec::new();
        let mut same_batch_pairs = HashSet::new();
        for &(left, right) in &batch.missing_edges {
            match (positions.contains(&left), positions.contains(&right)) {
                (true, false) => external_dependencies.push(right),
                (false, true) => external_dependencies.push(left),
                (true, true) => {
                    let pair = if left <= right {
                        (left, right)
                    } else {
                        (right, left)
                    };
                    same_batch_pairs.insert(pair);
                }
                _ => {}
            }
        }

        external_dependencies.sort_unstable();
        external_dependencies.dedup();
        MissingEdgeSummary {
            external_dependencies,
            same_batch_pair_count: same_batch_pairs.len(),
        }
    }

    fn track_pending_dependencies(&mut self, dependencies: &[u64]) {
        for dependency in dependencies {
            *self
                .pending_dependency_counts
                .entry(*dependency)
                .or_insert(0) += 1;
        }
    }

    fn untrack_pending_dependencies(&mut self, dependencies: &[u64]) {
        for dependency in dependencies {
            let should_remove = match self.pending_dependency_counts.get_mut(dependency) {
                Some(count) => {
                    *count -= 1;
                    *count == 0
                }
                None => false,
            };

            if should_remove {
                self.pending_dependency_counts.remove(dependency);
            }
        }
    }

    fn trim_processed_to_cap(&mut self, cap: usize) {
        let mut scanned = 0usize;
        while self.processed_fifo.len() > cap && scanned < self.processed_fifo.len() {
            let Some(oldest) = self.processed_fifo.pop_front() else {
                break;
            };

            if self.pending_dependency_counts.contains_key(&oldest) {
                self.processed_fifo.push_back(oldest);
                scanned += 1;
                continue;
            }

            self.processed_tx_ids.remove(&oldest);
            scanned = 0;
        }

        if self.processed_fifo.len() > cap {
            self.processed_trim_blocked_events += 1;
            if self.should_log_event(self.processed_trim_blocked_events) {
                warn!(
                    "Executor retained {} processed tx ids above cap {} because pending batches still depend on them",
                    self.processed_fifo.len(),
                    cap
                );
            }
        }
    }

    fn maybe_log_pending_queue_health(&mut self) {
        if self.pending_batches.is_empty() {
            return;
        }

        let oldest_sequence = self
            .pending_batches
            .iter()
            .map(|pending| pending.first_seen_sequence)
            .min()
            .unwrap_or(self.latest_sequence);
        let oldest_lag = self.latest_sequence.saturating_sub(oldest_sequence);
        let max_stalled_rounds = self
            .pending_batches
            .iter()
            .map(|pending| pending.stalled_rounds)
            .max()
            .unwrap_or(0);

        let unhealthy = self.pending_batches.len() > Self::MAX_PENDING_BATCHES_SOFT
            || oldest_lag > Self::MAX_PENDING_SEQUENCE_LAG_SOFT;
        if !unhealthy {
            return;
        }

        self.pending_health_events += 1;
        if self.should_log_event(self.pending_health_events) {
            warn!(
                "Executor pending queue length={} oldest_lag={} max_stalled_rounds={} pinned_dependencies={} dropped_observations={}",
                self.pending_batches.len(),
                oldest_lag,
                max_stalled_rounds,
                self.pending_dependency_counts.len(),
                self.dropped_observations,
            );
        }
    }

    fn should_log_event(&self, count: u64) -> bool {
        let interval = if cfg!(feature = "benchmark") {
            Self::BENCHMARK_HEALTH_LOG_INTERVAL
        } else {
            Self::HEALTH_LOG_INTERVAL
        };
        count == 1 || count % interval == 0
    }

    fn record_same_batch_fallback(
        &mut self,
        same_batch_pair_count: usize,
        sequence: u64,
        digest: &Digest,
    ) {
        if same_batch_pair_count == 0 {
            return;
        }

        self.same_batch_fallback_batches += 1;
        self.same_batch_fallback_pairs += same_batch_pair_count as u64;
        if self.should_log_event(self.same_batch_fallback_batches) {
            warn!(
                "Executor released {} batches with same-batch fallback ({} pairs total, last_pairs={}, sequence={}, digest={}, pending_queue={})",
                self.same_batch_fallback_batches,
                self.same_batch_fallback_pairs,
                same_batch_pair_count,
                sequence,
                digest,
                self.pending_batches.len(),
            );
        }
    }

    fn log_metrics_snapshot(&self) {
        #[cfg(feature = "benchmark")]
        {
            if !self.benchmark_log_batches {
                return;
            }

            let has_metrics = self.same_batch_fallback_batches > 0
                || self.same_batch_fallback_pairs > 0
                || self.dropped_observations > 0
                || self.pending_health_events > 0
                || self.processed_trim_blocked_events > 0
                || !self.pending_batches.is_empty();
            if !has_metrics {
                return;
            }

            info!(
                "ExecutorMetrics fallback_batches={} fallback_pairs={} dropped_observations={} pending_health_events={} processed_trim_blocked_events={} pending_batches={}",
                self.same_batch_fallback_batches,
                self.same_batch_fallback_pairs,
                self.dropped_observations,
                self.pending_health_events,
                self.processed_trim_blocked_events,
                self.pending_batches.len(),
            );
        }
    }

    #[cfg(feature = "benchmark")]
    fn log_executed_batch(digest: &Digest, transactions: &[Transaction]) {
        let size: usize = transactions.iter().map(|tx| tx.len()).sum();
        for tx in transactions {
            if tx.first() == Some(&0u8) && tx.len() > 8 {
                let id_bytes: [u8; 8] = tx[1..9]
                    .try_into()
                    .expect("sample transaction id should be 8 bytes");
                info!(
                    "ExecutedBatch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id_bytes)
                );
            }
        }

        info!("ExecutedBatch {:?} contains {} B", digest, size);
    }
}
