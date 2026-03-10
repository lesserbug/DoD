// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::batch_maker::Batch;
use crate::common::{committee_with_base_port, keys, listener, standard_transaction};
use bytes::Bytes;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use network::SimpleSender;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use std::fs;

#[tokio::test]
async fn handle_clients_transactions() {
    let (name, _) = keys().pop().unwrap();
    let id = 0;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let path = ".db_test_handle_clients_transactions";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();

    // Spawn a `Worker` instance.
    Worker::spawn(name, id, committee.clone(), parameters, store);

    // Compute the expected global graph digest.
    let expected_global_batch = Batch {
        author: name,
        sequence: 0,
        transactions: vec![standard_transaction(1, 7), standard_transaction(2, 7)],
        edges: vec![(1, 2)],
    };
    let expected_global =
        bincode::serialize(&WorkerMessage::GlobalBatch(expected_global_batch)).unwrap();
    let expected_digest = Digest(
        Sha512::digest(&expected_global).as_slice()[..32]
            .try_into()
            .unwrap(),
    );

    // Spawn a network listener to receive our global graph digest.
    let primary_address = committee.primary(&name).unwrap().worker_to_primary;
    let expected =
        bincode::serialize(&WorkerPrimaryMessage::OurBatch(expected_digest, id)).unwrap();
    let handle = listener(primary_address, Some(Bytes::from(expected)));

    // Spawn workers' listeners to acknowledge our local graph broadcast.
    for (_, addresses) in committee.others_workers(&name, &id) {
        let address = addresses.worker_to_worker;
        let _ = listener(address, /* expected */ None);
    }

    let mut network = SimpleSender::new();

    // Send enough transactions to create our local graph.
    let transactions_address = committee.worker(&name, &id).unwrap().transactions;
    network
        .send(
            transactions_address,
            Bytes::from(standard_transaction(1, 7)),
        )
        .await;
    network
        .send(
            transactions_address,
            Bytes::from(standard_transaction(2, 7)),
        )
        .await;

    // Deliver 2 additional local graphs for the same sequence so the worker reaches n-f.
    let worker_address = committee.worker(&name, &id).unwrap().worker_to_worker;
    for (peer, _) in committee.others_workers(&name, &id).into_iter().take(2) {
        let local_batch = Batch {
            author: peer,
            sequence: 0,
            transactions: vec![standard_transaction(1, 7), standard_transaction(2, 7)],
            edges: vec![(1, 2)],
        };
        let message = WorkerMessage::LocalBatch(local_batch);
        let serialized = bincode::serialize(&message).unwrap();
        network.send(worker_address, Bytes::from(serialized)).await;
    }

    // Ensure the primary received the global graph digest (ie. it did not panic).
    assert!(handle.await.is_ok());
}
