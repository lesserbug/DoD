// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::batch_maker::parse_transaction_id_and_state_key;
use crate::common::{committee_with_base_port, keys, standard_transaction};
use std::collections::HashMap;
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
        missing_edges: Vec::new(),
    };
    bincode::serialize(&WorkerMessage::LocalBatch(batch)).unwrap()
}

fn make_custom_local_graph(
    author: PublicKey,
    sequence: u64,
    transactions: Vec<Transaction>,
    edges: Vec<(u64, u64)>,
) -> SerializedBatchMessage {
    make_custom_local_graph_with_missing_edges(author, sequence, transactions, edges, Vec::new())
}

fn make_custom_local_graph_with_missing_edges(
    author: PublicKey,
    sequence: u64,
    transactions: Vec<Transaction>,
    edges: Vec<(u64, u64)>,
    missing_edges: Vec<(u64, u64)>,
) -> SerializedBatchMessage {
    let batch = Batch {
        author,
        sequence,
        transactions,
        edges,
        missing_edges,
    };
    bincode::serialize(&WorkerMessage::LocalBatch(batch)).unwrap()
}

fn make_global_batch_with_missing_edges(
    author: PublicKey,
    sequence: u64,
    transactions: Vec<Transaction>,
    edges: Vec<(u64, u64)>,
    missing_edges: Vec<(u64, u64)>,
) -> SerializedBatchMessage {
    let batch = Batch {
        author,
        sequence,
        transactions,
        edges,
        missing_edges,
    };
    bincode::serialize(&WorkerMessage::GlobalBatch(batch)).unwrap()
}

fn legacy_sample_transaction(id: u64) -> Transaction {
    let mut tx = Vec::with_capacity(9);
    tx.push(0u8);
    tx.extend_from_slice(&id.to_be_bytes());
    tx
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

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

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
async fn keeps_legacy_sample_transactions_in_global_graph() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(12_500);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    let own_graph = make_custom_local_graph(
        name,
        0,
        vec![legacy_sample_transaction(7), standard_transaction(8, 1)],
        vec![],
    );
    tx_own.send(own_graph).await.unwrap();

    for peer in peers {
        let peer_graph = make_custom_local_graph(
            peer,
            0,
            vec![legacy_sample_transaction(7), standard_transaction(8, 1)],
            vec![],
        );
        tx_workers.send(peer_graph).await.unwrap();
    }

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert_eq!(batch.sequence, 0);
            assert_eq!(tx_ids(&batch), vec![7, 8]);

            let sample = batch
                .transactions
                .iter()
                .find(|tx| tx.first() == Some(&0u8))
                .expect("legacy sample transaction should be preserved");
            assert_eq!(sample.len(), 9);
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

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

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
async fn records_missing_edges_when_support_is_ambiguous() {
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

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph(
            name,
            0,
            vec![standard_transaction(1, 5), standard_transaction(2, 5)],
            vec![(1, 2)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_custom_local_graph(
            peers[0],
            0,
            vec![standard_transaction(1, 5), standard_transaction(2, 5)],
            vec![(2, 1)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_custom_local_graph(
            peers[1],
            0,
            vec![standard_transaction(1, 5), standard_transaction(2, 5)],
            vec![],
        ))
        .await
        .unwrap();

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert!(batch.edges.is_empty());
            assert_eq!(batch.missing_edges, vec![(1, 2)]);
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn keeps_only_frontier_missing_edges_within_a_key() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_150);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph(
            name,
            0,
            vec![
                standard_transaction(1, 5),
                standard_transaction(2, 5),
                standard_transaction(3, 5),
            ],
            vec![],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_custom_local_graph(
                peer,
                0,
                vec![
                    standard_transaction(1, 5),
                    standard_transaction(2, 5),
                    standard_transaction(3, 5),
                ],
                vec![],
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
            assert!(batch.edges.is_empty());
            assert_eq!(batch.missing_edges, vec![(1, 2), (2, 3)]);
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn skips_missing_edges_after_linearizing_same_scc() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_200);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph(
            name,
            0,
            vec![
                standard_transaction(1, 5),
                standard_transaction(2, 5),
                standard_transaction(3, 5),
            ],
            vec![(1, 2), (2, 3), (3, 1)],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_custom_local_graph(
                peer,
                0,
                vec![
                    standard_transaction(1, 5),
                    standard_transaction(2, 5),
                    standard_transaction(3, 5),
                ],
                vec![(1, 2), (2, 3), (3, 1)],
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
            assert!(batch.missing_edges.is_empty());
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn forwards_local_missing_predecessors_into_global_batch() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_250);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph_with_missing_edges(
            name,
            0,
            vec![standard_transaction(43, 5)],
            vec![],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_custom_local_graph_with_missing_edges(
                peer,
                0,
                vec![standard_transaction(43, 5)],
                vec![],
                vec![(41, 43)],
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
            assert_eq!(tx_ids(&batch), vec![43]);
            assert_eq!(batch.missing_edges, vec![(41, 43)]);
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn upgrades_supported_in_batch_missing_predecessors_into_edges() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_275);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph_with_missing_edges(
            name,
            0,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_custom_local_graph_with_missing_edges(
                peer,
                0,
                vec![standard_transaction(41, 5), standard_transaction(43, 5)],
                vec![],
                vec![(41, 43)],
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
            assert_eq!(tx_ids(&batch), vec![41, 43]);
            assert_eq!(batch.edges, vec![(41, 43)]);
            assert!(batch.missing_edges.is_empty());
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn combines_edge_and_missing_support_when_promoting_in_batch_edges() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_282);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph(
            name,
            0,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_custom_local_graph_with_missing_edges(
            peers[0],
            0,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_custom_local_graph(
            peers[1],
            0,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![],
        ))
        .await
        .unwrap();

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert_eq!(tx_ids(&batch), vec![41, 43]);
            assert_eq!(batch.edges, vec![(41, 43)]);
            assert!(batch.missing_edges.is_empty());
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn accumulates_missing_support_across_global_rounds() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_286);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_workers_global, rx_workers_global) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn_with_global_sync_channel(
        name,
        committee,
        rx_own,
        rx_workers,
        rx_workers_global,
        tx_global,
        vec![],
    );

    tx_workers_global
        .send(make_global_batch_with_missing_edges(
            peers[0],
            0,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    tx_own
        .send(make_custom_local_graph(
            name,
            1,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_custom_local_graph(
            peers[0],
            1,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![],
        ))
        .await
        .unwrap();

    tx_workers
        .send(make_custom_local_graph(
            peers[1],
            1,
            vec![standard_transaction(41, 5), standard_transaction(43, 5)],
            vec![],
        ))
        .await
        .unwrap();

    let serialized = rx_global
        .recv()
        .await
        .expect("Global orderer did not output a global graph");

    match bincode::deserialize(&serialized).unwrap() {
        WorkerMessage::GlobalBatch(batch) => {
            assert_eq!(batch.sequence, 1);
            assert_eq!(tx_ids(&batch), vec![41, 43]);
            assert_eq!(batch.edges, vec![(41, 43)]);
            assert!(batch.missing_edges.is_empty());
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn keeps_only_frontier_promoted_in_batch_edges() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_290);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph_with_missing_edges(
            name,
            0,
            vec![
                standard_transaction(1, 5),
                standard_transaction(2, 5),
                standard_transaction(3, 5),
            ],
            vec![],
            vec![(1, 3), (2, 3)],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_custom_local_graph_with_missing_edges(
                peer,
                0,
                vec![
                    standard_transaction(1, 5),
                    standard_transaction(2, 5),
                    standard_transaction(3, 5),
                ],
                vec![],
                vec![(1, 3), (2, 3)],
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
            assert_eq!(batch.edges, vec![(2, 3)]);
            assert_eq!(batch.missing_edges, vec![(1, 2)]);
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[tokio::test]
async fn forwarded_missing_predecessors_need_multi_graph_support() {
    let (name, _) = keys().pop().unwrap();
    let committee = committee_with_base_port(14_300);
    let peers: Vec<_> = committee
        .others_workers(&name, &0)
        .into_iter()
        .map(|(peer, _)| peer)
        .take(2)
        .collect();

    let (tx_own, rx_own) = channel(10);
    let (tx_workers, rx_workers) = channel(10);
    let (tx_global, mut rx_global) = channel(10);

    GlobalOrderer::spawn(name, committee, rx_own, rx_workers, tx_global, vec![]);

    tx_own
        .send(make_custom_local_graph_with_missing_edges(
            name,
            0,
            vec![standard_transaction(43, 5)],
            vec![],
            vec![(41, 43)],
        ))
        .await
        .unwrap();

    for peer in peers {
        tx_workers
            .send(make_custom_local_graph_with_missing_edges(
                peer,
                0,
                vec![standard_transaction(43, 5)],
                vec![],
                Vec::new(),
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
            assert_eq!(tx_ids(&batch), vec![43]);
            assert!(batch.missing_edges.is_empty());
        }
        other => panic!("Unexpected worker message: {:?}", other),
    }
}

#[test]
fn selects_a_deterministic_quorum_subset() {
    let committee = committee_with_base_port(14_500);
    let mut authors: Vec<_> = committee.authorities.keys().copied().collect();
    authors.sort_unstable();

    let local_graphs: HashMap<_, _> = authors
        .iter()
        .enumerate()
        .rev()
        .map(|(index, author)| {
            (
                *author,
                Batch {
                    author: *author,
                    sequence: 7,
                    transactions: vec![standard_transaction(100 + index as u64, 1)],
                    edges: Vec::new(),
                    missing_edges: Vec::new(),
                },
            )
        })
        .collect();

    let selected = GlobalOrderer::select_quorum_graphs(&committee, &local_graphs);
    let selected_authors: Vec<_> = selected.iter().map(|batch| batch.author).collect();

    assert_eq!(selected.len(), committee.quorum_threshold() as usize);
    assert_eq!(
        selected_authors,
        authors[..committee.quorum_threshold() as usize].to_vec()
    );
}
