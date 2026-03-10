// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::batch_maker::parse_transaction_id_and_state_key;
use crate::common::{committee_with_base_port, keys, standard_transaction};
use tokio::sync::mpsc::channel;

fn make_local_graph(
    author: PublicKey,
    sequence: u64,
    tx_ids: &[u64],
    state_key: u8,
    edges: Vec<(u64, u64)>,
) -> SerializedBatchMessage {
    let transactions = tx_ids
        .iter()
        .map(|tx_id| standard_transaction(*tx_id, state_key))
        .collect();

    let batch = Batch {
        author,
        sequence,
        transactions,
        edges,
    };
    bincode::serialize(&WorkerMessage::LocalBatch(batch)).unwrap()
}

fn tx_ids(batch: &Batch) -> Vec<u64> {
    batch
        .transactions
        .iter()
        .filter_map(|tx| parse_transaction_id_and_state_key(tx).map(|(tx_id, _)| tx_id))
        .collect()
}

#[tokio::test]
async fn emits_global_batch_after_n_minus_f_local_graphs() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(12_000);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global);

    tx_own
        .send(make_local_graph(name, 0, &[10, 11], 7, vec![(10, 11)]))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_local_graph(peer, 0, &[10, 11], 7, vec![(10, 11)]))
            .await
            .unwrap();
    }

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert_eq!(batch.author, name);
            assert_eq!(batch.sequence, 0);
            assert_eq!(tx_ids(&batch), vec![10, 11]);
            assert_eq!(batch.edges, vec![(10, 11)]);
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn performs_transitive_reduction_on_global_graph() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(13_000);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global);

    tx_own
        .send(make_local_graph(
            name,
            0,
            &[1, 2, 3],
            9,
            vec![(1, 2), (2, 3), (1, 3)],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_local_graph(
                peer,
                0,
                &[1, 2, 3],
                9,
                vec![(1, 2), (2, 3), (1, 3)],
            ))
            .await
            .unwrap();
    }

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert_eq!(tx_ids(&batch), vec![1, 2, 3]);
            assert_eq!(batch.edges, vec![(1, 2), (2, 3)]);
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn removes_pending_to_fixed_edges() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_000);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global);

    tx_own
        .send(make_local_graph(
            name,
            0,
            &[1, 2, 3],
            5,
            vec![(1, 2), (2, 3), (1, 3)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_local_graph(
            peers[0],
            0,
            &[1, 2, 3],
            5,
            vec![(1, 2), (2, 3), (1, 3)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_local_graph(peers[1], 0, &[1, 3], 5, vec![(1, 3)]))
        .await
        .unwrap();

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert!(batch.edges.contains(&(1, 2)));
            assert!(batch.edges.contains(&(1, 3)));
            assert!(!batch.edges.contains(&(2, 3)));
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}
