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
use std::collections::{BTreeSet, HashMap, HashSet};
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
    // Each local graph only carries edges among the transactions that are
    // actually present in that graph.
    pub edges: Vec<(u64, u64)>,
    #[serde(default)]
    pub missing_edges: Vec<(u64, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalGraphInfo {
    pub sequence: u64,
    pub tx_ids: Vec<u64>,
    pub missing_edges: Vec<(u64, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchMakerControl {
    ObserveGlobalGraph(GlobalGraphInfo),
}

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ParsedStandardTx {
    tx_id: u64,
    state_key: u8,
}

#[derive(Clone, Debug)]
struct KnownTx {
    transaction: Transaction,
    first_seen_sequence: u64,
    last_seen_sequence: u64,
}

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
    /// Local control channel used to feed global-order observations back into
    /// the batch maker.
    rx_control: Receiver<BatchMakerControl>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<QuorumWaiterMessage>,
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch of fresh client transactions.
    current_batch: Vec<Transaction>,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: ReliableSender,
    /// Standard transactions that this worker has seen recently.
    known_transactions: HashMap<u64, KnownTx>,
    /// Transactions that remain unresolved and should be reintroduced into
    /// future local-order graphs.
    retained_unresolved: BTreeSet<u64>,
    /// A bounded local skeleton of `M_w`, currently tracking unresolved pairs
    /// derived from recent global-order messages.
    missing_edge_store: HashMap<(u64, u64), u64>,
    /// Sequence number of the next local-order graph.
    next_sequence: u64,
}

impl BatchMaker {
    const MAX_KNOWN_TX_HISTORY_SEQUENCES: u64 = 32;
    const MAX_MISSING_EDGE_HISTORY_SEQUENCES: u64 = 32;
    const MAX_RETAINED_UNRESOLVED_TXS: usize = 512;
    const MAX_CARRYOVER_TXS_PER_BATCH: usize = 256;

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
                known_transactions: HashMap::new(),
                retained_unresolved: BTreeSet::new(),
                missing_edge_store: HashMap::new(),
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
                    if let Some((tx_id, _)) = parse_standard_transaction(&transaction) {
                        self.record_known_transaction(self.next_sequence, tx_id, transaction.clone());
                    }
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
                    if !self.current_batch.is_empty() || !self.retained_unresolved.is_empty() {
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

        let sequence = self.next_sequence;
        self.prune_local_state(sequence);

        self.current_batch_size = 0;
        let fresh_transactions: Vec<_> = self.current_batch.drain(..).collect();
        let transactions = self.compose_transactions_for_sequence(fresh_transactions);

        let standard_txs: Vec<_> = transactions
            .iter()
            .filter_map(|tx| {
                parse_standard_transaction(tx)
                    .map(|(tx_id, state_key)| ParsedStandardTx { tx_id, state_key })
            })
            .collect();

        let mut edge_set = HashSet::new();
        let mut batch_writers: HashMap<u8, Vec<u64>> = HashMap::new();
        for tx in &standard_txs {
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

    fn compose_transactions_for_sequence(
        &self,
        fresh_transactions: Vec<Transaction>,
    ) -> Vec<Transaction> {
        let mut transactions = Vec::new();
        let mut included_standard = HashSet::new();
        let mut carryover_bytes = 0usize;

        for tx_id in self.retained_unresolved_ids() {
            if transactions.len() >= Self::MAX_CARRYOVER_TXS_PER_BATCH
                || carryover_bytes >= self.batch_size
            {
                break;
            }

            let Some(known) = self.known_transactions.get(&tx_id) else {
                continue;
            };
            if included_standard.insert(tx_id) {
                carryover_bytes += known.transaction.len();
                transactions.push(known.transaction.clone());
            }
        }

        for transaction in fresh_transactions {
            match parse_standard_transaction(&transaction) {
                Some((tx_id, _)) if included_standard.insert(tx_id) => {
                    transactions.push(transaction)
                }
                Some(..) => {}
                None => transactions.push(transaction),
            }
        }

        transactions
    }

    fn retained_unresolved_ids(&self) -> Vec<u64> {
        let mut tx_ids: Vec<_> = self.retained_unresolved.iter().copied().collect();
        tx_ids.sort_unstable_by_key(|tx_id| {
            self.known_transactions
                .get(tx_id)
                .map(|known| (known.first_seen_sequence, *tx_id))
                .unwrap_or((u64::MAX, *tx_id))
        });
        tx_ids
    }

    fn handle_control(&mut self, control: BatchMakerControl) {
        match control {
            BatchMakerControl::ObserveGlobalGraph(info) => {
                let observed_txs: HashSet<_> = info.tx_ids.iter().copied().collect();
                let observed_pairs: HashSet<_> = info
                    .missing_edges
                    .iter()
                    .map(|&(left, right)| canonical_missing_edge(left, right))
                    .collect();

                self.missing_edge_store.retain(|&(left, right), _| {
                    if observed_txs.contains(&left) && observed_txs.contains(&right) {
                        observed_pairs.contains(&(left, right))
                    } else {
                        true
                    }
                });
                for pair in observed_pairs {
                    self.missing_edge_store.insert(pair, info.sequence);
                }

                self.refresh_retained_unresolved();
            }
        }

        self.prune_local_state(self.next_sequence);
    }

    fn refresh_retained_unresolved(&mut self) {
        self.retained_unresolved = self
            .missing_edge_store
            .keys()
            .flat_map(|(left, right)| [*left, *right])
            .filter(|tx_id| self.known_transactions.contains_key(tx_id))
            .collect();
    }

    fn record_known_transaction(&mut self, sequence: u64, tx_id: u64, transaction: Transaction) {
        self.known_transactions
            .entry(tx_id)
            .and_modify(|known| {
                known.transaction = transaction.clone();
                known.last_seen_sequence = sequence;
            })
            .or_insert(KnownTx {
                transaction,
                first_seen_sequence: sequence,
                last_seen_sequence: sequence,
            });
    }

    fn prune_local_state(&mut self, current_sequence: u64) {
        let min_known_sequence =
            current_sequence.saturating_sub(Self::MAX_KNOWN_TX_HISTORY_SEQUENCES.saturating_sub(1));
        let retained_unresolved = self.retained_unresolved.clone();
        self.known_transactions.retain(|tx_id, known| {
            retained_unresolved.contains(tx_id) || known.last_seen_sequence >= min_known_sequence
        });

        let known_tx_ids: HashSet<_> = self.known_transactions.keys().copied().collect();
        self.retained_unresolved
            .retain(|tx_id| known_tx_ids.contains(tx_id));

        if self.retained_unresolved.len() > Self::MAX_RETAINED_UNRESOLVED_TXS {
            let mut ordered: Vec<_> = self.retained_unresolved.iter().copied().collect();
            ordered.sort_unstable_by_key(|tx_id| {
                self.known_transactions
                    .get(tx_id)
                    .map(|known| (known.first_seen_sequence, *tx_id))
                    .unwrap_or((u64::MAX, *tx_id))
            });
            self.retained_unresolved = ordered
                .into_iter()
                .take(Self::MAX_RETAINED_UNRESOLVED_TXS)
                .collect();
        }

        let min_missing_sequence = current_sequence
            .saturating_sub(Self::MAX_MISSING_EDGE_HISTORY_SEQUENCES.saturating_sub(1));
        self.missing_edge_store.retain(|(left, right), sequence| {
            *sequence >= min_missing_sequence
                && known_tx_ids.contains(left)
                && known_tx_ids.contains(right)
        });
    }
}
