// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{
    parse_standard_transaction, parse_transaction_id_and_state_key, Batch, BatchMakerControl,
    Transaction,
};
use crate::worker::WorkerMessage;
use config::WorkerId;
use crypto::Digest;
#[cfg(feature = "benchmark")]
use log::info;
use log::warn;
use std::collections::{BTreeSet, HashMap, HashSet};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/executor_tests.rs"]
pub mod executor_tests;

/// Executes globally ordered batches once the primary feeds back their ordered
/// digests. This is the worker-side skeleton of DoD Algorithm 3.
pub struct Executor {
    /// Our worker id.
    id: WorkerId,
    /// Persistent batch store shared with the processor.
    store: Store,
    /// Ordered digests to execute, as fed back by the primary.
    rx_execute: Receiver<Vec<(Digest, WorkerId)>>,
    /// Processed tx feedback sent back to the local batch maker.
    tx_batch_control: Sender<BatchMakerControl>,
    /// Avoid re-executing the same committed digest.
    executed: HashSet<Digest>,
    /// Whether to emit benchmark execution logs.
    benchmark_log_batches: bool,
}

impl Executor {
    pub fn spawn(
        id: WorkerId,
        store: Store,
        rx_execute: Receiver<Vec<(Digest, WorkerId)>>,
        tx_batch_control: Sender<BatchMakerControl>,
        benchmark_log_batches: bool,
    ) {
        tokio::spawn(async move {
            Self {
                id,
                store,
                rx_execute,
                tx_batch_control,
                executed: HashSet::new(),
                benchmark_log_batches,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(ordered_batches) = self.rx_execute.recv().await {
            for (digest, worker_id) in ordered_batches {
                if worker_id != self.id || !self.executed.insert(digest.clone()) {
                    continue;
                }

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

                let executed_transactions = Self::execute_batch(&batch);
                let processed_tx_ids = Self::collect_processed_tx_ids(&executed_transactions);
                if !processed_tx_ids.is_empty() {
                    self.tx_batch_control
                        .send(BatchMakerControl::mark_processed(processed_tx_ids))
                        .await
                        .expect("Failed to send processed feedback to batch maker");
                }
                #[cfg(not(feature = "benchmark"))]
                let _ = (&executed_transactions, self.benchmark_log_batches);
                #[cfg(feature = "benchmark")]
                if self.benchmark_log_batches {
                    Self::log_executed_batch(&digest, &executed_transactions);
                }
            }
        }
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

        // Until the full M_w / processed feedback loop exists, we deterministically
        // orient the remaining missing pairs by the already finalized batch order.
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

    fn collect_processed_tx_ids(transactions: &[Transaction]) -> Vec<u64> {
        let mut tx_ids: Vec<_> = transactions
            .iter()
            .filter_map(|tx| parse_standard_transaction(tx).map(|(tx_id, _)| tx_id))
            .collect();
        tx_ids.sort_unstable();
        tx_ids.dedup();
        tx_ids
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
