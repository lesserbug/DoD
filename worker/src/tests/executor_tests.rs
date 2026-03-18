// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crypto::PublicKey;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use store::Store;
use tokio::sync::mpsc::channel;

fn standard_transaction(id: u64, state_key: u8) -> Transaction {
    let mut tx = Vec::with_capacity(100);
    tx.push(1u8);
    tx.extend_from_slice(&id.to_be_bytes());
    tx.push(state_key);
    tx.resize(100, 0u8);
    tx
}

fn legacy_sample_transaction(id: u64) -> Transaction {
    let mut tx = Vec::with_capacity(9);
    tx.push(0u8);
    tx.extend_from_slice(&id.to_be_bytes());
    tx
}

#[test]
fn executes_batches_with_kahn_order() {
    let batch = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: vec![
            standard_transaction(3, 1),
            standard_transaction(1, 1),
            standard_transaction(2, 1),
        ],
        edges: vec![(1, 2)],
        missing_edges: vec![(1, 3), (2, 3)],
    };

    let ordered = Executor::execute_batch(&batch);
    let tx_ids: Vec<_> = ordered
        .iter()
        .filter_map(|tx| parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| tx_id))
        .collect();

    assert_eq!(tx_ids, vec![3, 1, 2]);
}

#[test]
fn preserves_singleton_batches() {
    let batch = Batch {
        author: PublicKey::default(),
        sequence: 7,
        transactions: vec![standard_transaction(11, 9)],
        edges: Vec::new(),
        missing_edges: vec![(11, 11)],
    };

    let ordered = Executor::execute_batch(&batch);
    let tx_ids: Vec<_> = ordered
        .iter()
        .filter_map(|tx| parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| tx_id))
        .collect();

    assert_eq!(tx_ids, vec![11]);
}

#[test]
fn processed_feedback_only_tracks_standard_transactions() {
    let tx_ids = Executor::collect_processed_tx_ids(&[
        legacy_sample_transaction(1),
        standard_transaction(3, 9),
        standard_transaction(2, 9),
        standard_transaction(3, 9),
    ]);

    assert_eq!(tx_ids, vec![2, 3]);
}

#[test]
fn queues_batches_with_unprocessed_external_missing_predecessors() {
    let batch = Batch {
        author: PublicKey::default(),
        sequence: 3,
        transactions: vec![standard_transaction(43, 9)],
        edges: Vec::new(),
        missing_edges: vec![(41, 43)],
    };
    let summary = Executor::summarize_missing_edges(&batch);

    assert_eq!(summary.external_dependencies, vec![41]);
    assert_eq!(summary.same_batch_pair_count, 0);
    assert!(!Executor::batch_ready(
        &summary.external_dependencies,
        &HashSet::new()
    ));

    let mut processed = HashSet::new();
    processed.insert(41);
    assert!(Executor::batch_ready(
        &summary.external_dependencies,
        &processed
    ));
}

#[test]
fn external_missing_predecessors_ignore_same_batch_pairs() {
    let batch = Batch {
        author: PublicKey::default(),
        sequence: 4,
        transactions: vec![standard_transaction(43, 9), standard_transaction(44, 9)],
        edges: Vec::new(),
        missing_edges: vec![(41, 43), (43, 44), (44, 88)],
    };
    let summary = Executor::summarize_missing_edges(&batch);

    assert_eq!(summary.external_dependencies, vec![41, 88]);
    assert_eq!(summary.same_batch_pair_count, 1);
}

#[test]
fn same_batch_missing_pairs_do_not_block_batch_readiness() {
    let batch = Batch {
        author: PublicKey::default(),
        sequence: 5,
        transactions: vec![standard_transaction(43, 9), standard_transaction(44, 9)],
        edges: Vec::new(),
        missing_edges: vec![(43, 44)],
    };
    let summary = Executor::summarize_missing_edges(&batch);

    assert!(summary.external_dependencies.is_empty());
    assert_eq!(summary.same_batch_pair_count, 1);
    assert!(Executor::batch_ready(
        &summary.external_dependencies,
        &HashSet::new()
    ));
}

#[tokio::test]
async fn trim_processed_to_cap_keeps_pending_dependencies_pinned() {
    let path = ".db_test_executor_trim_processed_to_cap_keeps_pending_dependencies_pinned";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (_tx_execute, rx_execute) = channel(1);
    let (tx_observation, _rx_observation) = channel(1);
    let (tx_processed, _rx_processed) = channel(1);

    let mut executor = Executor {
        id: 0,
        store,
        rx_execute,
        tx_batch_observation: tx_observation,
        tx_batch_processed: tx_processed,
        executed: HashSet::new(),
        processed_tx_ids: HashSet::from([41, 42]),
        processed_fifo: VecDeque::from([41, 42]),
        pending_batches: VecDeque::new(),
        pending_dependency_counts: HashMap::from([(41, 1usize)]),
        latest_sequence: 0,
        dropped_observations: 0,
        pending_health_events: 0,
        processed_trim_blocked_events: 0,
        same_batch_fallback_batches: 0,
        same_batch_fallback_pairs: 0,
        benchmark_log_batches: false,
    };

    executor.trim_processed_to_cap(1);

    assert!(executor.processed_tx_ids.contains(&41));
    assert!(!executor.processed_tx_ids.contains(&42));
    assert_eq!(
        executor.processed_fifo.iter().copied().collect::<Vec<_>>(),
        vec![41]
    );
}
