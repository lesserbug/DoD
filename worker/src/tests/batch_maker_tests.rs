// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::transaction;
use tokio::sync::mpsc::channel;

fn standard_transaction(id: u64, state_key: u8) -> Transaction {
    let mut tx = Vec::with_capacity(100);
    tx.push(1u8);
    tx.extend_from_slice(&id.to_be_bytes());
    tx.push(state_key);
    tx.resize(100, 0u8);
    tx
}

#[tokio::test]
async fn make_batch() {
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000, // Ensure the timer is not triggered.
        rx_transaction,
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
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 50, // Ensure the timer is triggered.
        rx_transaction,
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
    let (tx_message, mut rx_message) = channel(1);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 200,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
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
async fn local_order_persists_across_batches() {
    let (tx_transaction, rx_transaction) = channel(2);
    let (tx_message, mut rx_message) = channel(2);
    let dummy_addresses = vec![(PublicKey::default(), "127.0.0.1:0".parse().unwrap())];

    BatchMaker::spawn(
        PublicKey::default(),
        /* max_batch_size */ 100,
        /* max_batch_delay */ 1_000_000,
        rx_transaction,
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
            assert_eq!(batch.edges, vec![(42, 43)]);
        }
        _ => panic!("Unexpected message"),
    }
}
