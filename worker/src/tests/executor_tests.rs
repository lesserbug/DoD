// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crypto::PublicKey;

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
