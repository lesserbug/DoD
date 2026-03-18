// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use network::ReliableSender;
use std::collections::{HashMap, HashSet, VecDeque};
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

fn batch_tx_ids(batch: &Batch) -> Vec<u64> {
    batch
        .transactions
        .iter()
        .filter_map(|tx| parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| tx_id))
        .collect()
}

fn queued_unprocessed_ids(batch_maker: &BatchMaker, state_key: u8) -> Option<Vec<u64>> {
    batch_maker
        .unprocessed_by_key
        .get(&state_key)
        .map(|tx_ids| tx_ids.iter().copied().collect())
}

#[test]
fn parses_legacy_sample_transaction_layout() {
    let tx = legacy_sample_transaction(42);
    assert_eq!(parse_transaction_id_and_state_key(&tx), Some((42, 0u8)));
    assert_eq!(parse_standard_transaction(&tx), None);
}

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (_tx_control, rx_control) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
        rx_control,
        tx_message,
        /* workers_addresses */ dummy_addresses,
    );

    // Send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: vec![transaction(), transaction()],
        edges: Vec::new(),
        missing_edges: Vec::new(),
    };
    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (_tx_control, rx_control) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
        rx_control,
        tx_message,
        /* workers_addresses */ dummy_addresses,
    );

    // Do not send enough transactions to seal a batch.
    tx_transaction.send(transaction()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: vec![transaction()],
        edges: Vec::new(),
        missing_edges: Vec::new(),
    };
    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn local_order_adds_edge_for_conflicting_txs_in_same_batch() {
    let (tx_transaction, rx_transaction) = channel(2);
    let (_tx_control, rx_control) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        rx_control,
        tx_message,
        dummy_addresses,
    );

    tx_transaction
        .send(standard_transaction(10, 7))
        .await
        .unwrap();
    tx_transaction
        .send(standard_transaction(11, 7))
        .await
        .unwrap();

    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 0);
            assert_eq!(batch.edges, vec![(10, 11)]);
        }
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn local_order_links_to_all_prior_conflicting_txs() {
    let (tx_transaction, rx_transaction) = channel(3);
    let (_tx_control, rx_control) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 300,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        rx_control,
        tx_message,
        dummy_addresses,
    );

    tx_transaction
        .send(standard_transaction(10, 7))
        .await
        .unwrap();
    tx_transaction
        .send(standard_transaction(11, 7))
        .await
        .unwrap();
    tx_transaction
        .send(standard_transaction(12, 7))
        .await
        .unwrap();

    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 0);
            assert_eq!(batch.edges, vec![(10, 11), (10, 12), (11, 12)]);
        }
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn local_order_keeps_cross_batch_last_writer_without_rebroadcasting_unresolved_txs() {
    let (tx_transaction, rx_transaction) = channel(3);
    let (_tx_control, rx_control) = channel(1);
    let (tx_message, mut rx_message) = channel(3);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 100,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        rx_control,
        tx_message,
        dummy_addresses,
    );

    tx_transaction
        .send(standard_transaction(42, 9))
        .await
        .unwrap();
    let QuorumWaiterMessage {
        batch: first_batch,
        handlers: _,
    } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&first_batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 0);
            assert_eq!(batch_tx_ids(&batch), vec![42]);
            assert!(batch.edges.is_empty());
        }
        _ => panic!("Unexpected message"),
    }

    tx_transaction
        .send(standard_transaction(43, 9))
        .await
        .unwrap();
    let QuorumWaiterMessage {
        batch: second_batch,
        handlers: _,
    } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&second_batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 1);
            assert_eq!(batch_tx_ids(&batch), vec![43]);
            assert_eq!(batch.edges, vec![(42, 43)]);
        }
        _ => panic!("Unexpected message"),
    }

    tx_transaction
        .send(standard_transaction(44, 9))
        .await
        .unwrap();
    let QuorumWaiterMessage {
        batch: third_batch,
        handlers: _,
    } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&third_batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 2);
            assert_eq!(batch_tx_ids(&batch), vec![44]);
            assert_eq!(batch.edges, vec![(43, 44)]);
        }
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn observe_global_graph_turns_ambiguous_unprocessed_txs_into_local_missing_edges() {
    let (tx_transaction, rx_transaction) = channel(4);
    let (tx_observation_control, rx_observation_control) = channel(4);
    let (_tx_processed_control, rx_processed_control) = channel(4);
    let (tx_message, mut rx_message) = channel(4);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn_with_control_channels(
        PublicKey::default(),
        /* max_batch_size */ 100,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        dummy_addresses,
    );

    tx_transaction
        .send(standard_transaction(41, 9))
        .await
        .unwrap();
    let _ = rx_message.recv().await.unwrap();

    tx_transaction
        .send(standard_transaction(42, 9))
        .await
        .unwrap();
    let _ = rx_message.recv().await.unwrap();

    let observed_batch = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: vec![standard_transaction(41, 9), standard_transaction(500, 9)],
        edges: Vec::new(),
        missing_edges: vec![(41, 500)],
    };
    tx_observation_control
        .send(BatchMakerControl::observe_global_batch(&observed_batch))
        .await
        .unwrap();

    tx_transaction
        .send(standard_transaction(43, 9))
        .await
        .unwrap();

    let QuorumWaiterMessage {
        batch: third_batch,
        handlers: _,
    } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&third_batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 2);
            assert_eq!(batch_tx_ids(&batch), vec![43]);
            assert_eq!(batch.edges, vec![(42, 43)]);
            assert_eq!(batch.missing_edges, vec![(41, 43)]);
        }
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn processed_feedback_clears_local_missing_edge_carry_over() {
    let (tx_transaction, rx_transaction) = channel(4);
    let (tx_observation_control, rx_observation_control) = channel(4);
    let (tx_processed_control, rx_processed_control) = channel(4);
    let (tx_message, mut rx_message) = channel(4);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn_with_control_channels(
        PublicKey::default(),
        /* max_batch_size */ 100,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        dummy_addresses,
    );

    tx_transaction
        .send(standard_transaction(41, 9))
        .await
        .unwrap();
    let _ = rx_message.recv().await.unwrap();

    tx_transaction
        .send(standard_transaction(42, 9))
        .await
        .unwrap();
    let _ = rx_message.recv().await.unwrap();

    let observed_batch = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: vec![standard_transaction(41, 9), standard_transaction(500, 9)],
        edges: Vec::new(),
        missing_edges: vec![(41, 500)],
    };
    tx_observation_control
        .send(BatchMakerControl::observe_global_batch(&observed_batch))
        .await
        .unwrap();
    tx_processed_control
        .send(BatchMakerControl::mark_processed(vec![41]))
        .await
        .unwrap();

    tx_transaction
        .send(standard_transaction(43, 9))
        .await
        .unwrap();

    let QuorumWaiterMessage {
        batch: third_batch,
        handlers: _,
    } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&third_batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 2);
            assert_eq!(batch.edges, vec![(42, 43)]);
            assert!(batch.missing_edges.is_empty());
        }
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn local_order_prefers_an_unresolved_last_writer_as_missing_predecessor() {
    let (tx_transaction, rx_transaction) = channel(5);
    let (tx_observation_control, rx_observation_control) = channel(5);
    let (_tx_processed_control, rx_processed_control) = channel(5);
    let (tx_message, mut rx_message) = channel(5);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn_with_control_channels(
        PublicKey::default(),
        /* max_batch_size */ 100,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        dummy_addresses,
    );

    tx_transaction
        .send(standard_transaction(41, 9))
        .await
        .unwrap();
    let _ = rx_message.recv().await.unwrap();

    tx_transaction
        .send(standard_transaction(42, 9))
        .await
        .unwrap();
    let _ = rx_message.recv().await.unwrap();

    let first_observation = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: vec![standard_transaction(41, 9), standard_transaction(500, 9)],
        edges: Vec::new(),
        missing_edges: vec![(41, 500)],
    };
    tx_observation_control
        .send(BatchMakerControl::observe_global_batch(&first_observation))
        .await
        .unwrap();

    let second_observation = Batch {
        author: PublicKey::default(),
        sequence: 1,
        transactions: vec![standard_transaction(42, 9), standard_transaction(600, 9)],
        edges: Vec::new(),
        missing_edges: vec![(42, 600)],
    };
    tx_observation_control
        .send(BatchMakerControl::observe_global_batch(&second_observation))
        .await
        .unwrap();

    tx_transaction
        .send(standard_transaction(43, 9))
        .await
        .unwrap();

    let QuorumWaiterMessage {
        batch: third_batch,
        handlers: _,
    } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&third_batch).unwrap() {
        WorkerMessage::LocalBatch(batch) => {
            assert_eq!(batch.sequence, 2);
            assert!(batch.edges.is_empty());
            assert_eq!(batch.missing_edges, vec![(42, 43)]);
        }
        _ => panic!("Unexpected message"),
    }
}

#[test]
fn observe_global_batch_canonicalizes_missing_edges() {
    let batch = Batch {
        author: PublicKey::default(),
        sequence: 7,
        transactions: vec![
            standard_transaction(43, 4),
            legacy_sample_transaction(99),
            standard_transaction(42, 4),
        ],
        edges: Vec::new(),
        missing_edges: vec![(43, 42), (42, 43), (100, 101)],
    };

    match BatchMakerControl::observe_global_batch(&batch) {
        BatchMakerControl::ObserveGlobalGraph(info) => {
            assert_eq!(info.sequence, 7);
            assert_eq!(info.tx_ids, vec![42, 43]);
            assert_eq!(info.missing_edges, vec![(42, 43), (100, 101)]);
        }
        BatchMakerControl::MarkProcessed(_) => panic!("unexpected processed feedback control"),
    }
}

#[test]
fn drops_duplicate_unprocessed_standard_transactions() {
    let (_tx_transaction, rx_transaction) = channel(1);
    let (_tx_observation_control, rx_observation_control) = channel(1);
    let (_tx_processed_control, rx_processed_control) = channel(1);
    let (tx_message, _rx_message) = channel(1);

    let mut batch_maker = BatchMaker {
        name: PublicKey::default(),
        batch_size: 1,
        max_batch_delay: 1,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        workers_addresses: Vec::new(),
        current_batch: Vec::new(),
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
    };

    assert!(batch_maker.accept_transaction(standard_transaction(41, 9)));
    assert!(!batch_maker.accept_transaction(standard_transaction(41, 9)));
    assert!(batch_maker.accept_transaction(standard_transaction(42, 9)));

    let batch = Batch {
        author: PublicKey::default(),
        sequence: 0,
        transactions: batch_maker.current_batch.clone(),
        edges: Vec::new(),
        missing_edges: Vec::new(),
    };
    assert_eq!(batch_tx_ids(&batch), vec![41, 42]);
}

#[test]
fn drops_reappearing_transactions_while_they_are_still_unprocessed() {
    let (_tx_transaction, rx_transaction) = channel(1);
    let (_tx_observation_control, rx_observation_control) = channel(1);
    let (_tx_processed_control, rx_processed_control) = channel(1);
    let (tx_message, _rx_message) = channel(1);

    let mut batch_maker = BatchMaker {
        name: PublicKey::default(),
        batch_size: 1,
        max_batch_delay: 1,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        workers_addresses: Vec::new(),
        current_batch: Vec::new(),
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
    };

    batch_maker.record_unprocessed(41, 9);

    assert_eq!(batch_maker.tx_state(41), TxState::Unprocessed);
    assert!(!batch_maker.accept_transaction(standard_transaction(41, 9)));
    assert!(batch_maker.current_batch.is_empty());
}

#[test]
fn processed_feedback_marks_processed_state_for_control_path() {
    let (_tx_transaction, rx_transaction) = channel(1);
    let (_tx_observation_control, rx_observation_control) = channel(1);
    let (_tx_processed_control, rx_processed_control) = channel(1);
    let (tx_message, _rx_message) = channel(1);

    let mut batch_maker = BatchMaker {
        name: PublicKey::default(),
        batch_size: 1,
        max_batch_delay: 1,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        workers_addresses: Vec::new(),
        current_batch: Vec::new(),
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
    };

    batch_maker.record_unprocessed(41, 9);
    batch_maker.handle_control(BatchMakerControl::mark_processed(vec![41]));

    assert!(batch_maker.processed_tx_ids.contains(&41));
    assert_eq!(batch_maker.tx_state(41), TxState::Processed);

    batch_maker.observe_global_graph(GlobalGraphInfo {
        sequence: 0,
        tx_ids: vec![41, 500],
        missing_edges: vec![(41, 500)],
    });
    assert!(batch_maker.missing_pairs.is_empty());
}

#[test]
fn processed_feedback_prunes_unprocessed_state_without_retaining_payloads() {
    let (_tx_transaction, rx_transaction) = channel(1);
    let (_tx_observation_control, rx_observation_control) = channel(1);
    let (_tx_processed_control, rx_processed_control) = channel(1);
    let (tx_message, _rx_message) = channel(1);

    let mut batch_maker = BatchMaker {
        name: PublicKey::default(),
        batch_size: 1,
        max_batch_delay: 1,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        workers_addresses: Vec::new(),
        current_batch: Vec::new(),
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
    };

    batch_maker.record_unprocessed(41, 9);
    batch_maker.record_unprocessed(42, 9);
    batch_maker.record_unprocessed(100, 7);
    batch_maker.handle_control(BatchMakerControl::mark_processed(vec![42, 999]));

    assert_eq!(batch_maker.known_transactions.get(&41), Some(&9));
    assert_eq!(batch_maker.known_transactions.get(&42), None);
    assert_eq!(batch_maker.known_transactions.get(&100), Some(&7));
    assert_eq!(queued_unprocessed_ids(&batch_maker, 9), Some(vec![41, 42]));
    assert_eq!(queued_unprocessed_ids(&batch_maker, 7), Some(vec![100]));
}

#[test]
fn drain_control_backlog_limits_work_not_whole_messages() {
    let (_tx_transaction, rx_transaction) = channel(1);
    let (_tx_observation_control, rx_observation_control) = channel(1);
    let (_tx_processed_control, rx_processed_control) = channel(1);
    let (tx_message, _rx_message) = channel(1);

    let mut batch_maker = BatchMaker {
        name: PublicKey::default(),
        batch_size: 1,
        max_batch_delay: 1,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        workers_addresses: Vec::new(),
        current_batch: Vec::new(),
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
    };

    batch_maker.record_unprocessed(41, 9);
    batch_maker.record_unprocessed(42, 9);
    batch_maker.enqueue_control(BatchMakerControl::mark_processed(vec![41, 42]));

    batch_maker.drain_control_backlog(1);

    assert!(batch_maker.processed_tx_ids.contains(&41));
    assert!(!batch_maker.processed_tx_ids.contains(&42));
    assert_eq!(batch_maker.known_transactions.get(&41), None);
    assert_eq!(batch_maker.known_transactions.get(&42), Some(&9));
    assert_eq!(batch_maker.pending_processed_controls.len(), 1);

    batch_maker.drain_control_backlog(1);
    assert!(batch_maker.processed_tx_ids.contains(&42));
    assert!(batch_maker.pending_processed_controls.is_empty());
}

#[test]
fn unresolved_frontier_prunes_stale_frontier_candidates() {
    let (_tx_transaction, rx_transaction) = channel(1);
    let (_tx_observation_control, rx_observation_control) = channel(1);
    let (_tx_processed_control, rx_processed_control) = channel(1);
    let (tx_message, _rx_message) = channel(1);

    let mut batch_maker = BatchMaker {
        name: PublicKey::default(),
        batch_size: 1,
        max_batch_delay: 1,
        rx_transaction,
        rx_observation_control,
        rx_processed_control,
        tx_message,
        workers_addresses: Vec::new(),
        current_batch: Vec::new(),
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
    };

    for tx_id in 1..=64 {
        batch_maker.record_unprocessed(tx_id, 9);
    }
    batch_maker.observe_global_graph(GlobalGraphInfo {
        sequence: 0,
        tx_ids: vec![1, 33, 500, 600],
        missing_edges: vec![(1, 500), (33, 600)],
    });
    batch_maker.handle_control(BatchMakerControl::mark_processed(vec![1]));

    assert_eq!(batch_maker.unresolved_frontier_for_key(9), Some(33));
    assert_eq!(
        batch_maker
            .frontier_by_key
            .get(&9)
            .map(|queue| queue.iter().copied().collect::<Vec<_>>()),
        Some(vec![33])
    );
}
