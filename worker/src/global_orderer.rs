// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{parse_transaction_id_and_state_key, Batch, Transaction};
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
use config::{Committee, Stake};
use crypto::PublicKey;
use log::{debug, warn};
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/global_orderer_tests.rs"]
pub mod global_orderer_tests;

#[derive(Default)]
struct SequenceState {
    own_received: bool,
    collected_stake: Stake,
    local_graphs: HashMap<PublicKey, Batch>,
}

/// Collects local-order graphs and produces a global-order graph (DoD Algorithm 2).
pub struct GlobalOrderer {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Receives our own local-order graphs (already available at n-f workers).
    rx_own_local: Receiver<SerializedBatchMessage>,
    /// Receives local-order graphs broadcast by other workers.
    rx_workers_local: Receiver<SerializedBatchMessage>,
    /// Outputs serialized `WorkerMessage::GlobalBatch` graphs.
    tx_global: Sender<SerializedBatchMessage>,
    /// Per-sequence collection state.
    sequences: HashMap<u64, SequenceState>,
    /// Finalized sequences are ignored if duplicated later.
    finalized: HashSet<u64>,
}

impl GlobalOrderer {
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        rx_own_local: Receiver<SerializedBatchMessage>,
        rx_workers_local: Receiver<SerializedBatchMessage>,
        tx_global: Sender<SerializedBatchMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                rx_own_local,
                rx_workers_local,
                tx_global,
                sequences: HashMap::new(),
                finalized: HashSet::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(serialized) = self.rx_own_local.recv() => {
                    self.handle_local_graph(serialized, true).await;
                }
                Some(serialized) = self.rx_workers_local.recv() => {
                    self.handle_local_graph(serialized, false).await;
                }
                else => {
                    break;
                }
            }
        }
    }

    async fn handle_local_graph(
        &mut self,
        serialized: SerializedBatchMessage,
        from_our_quorum: bool,
    ) {
        let batch = match bincode::deserialize(&serialized) {
            Ok(WorkerMessage::LocalBatch(batch)) => batch,
            Ok(other) => {
                warn!(
                    "GlobalOrderer received unexpected worker message: {:?}",
                    other
                );
                return;
            }
            Err(error) => {
                warn!("GlobalOrderer failed to deserialize local graph: {}", error);
                return;
            }
        };

        if self.finalized.contains(&batch.sequence) {
            return;
        }

        let author_stake = self.committee.stake(&batch.author);
        if author_stake == 0 {
            warn!(
                "Ignoring local graph from unknown authority {}",
                batch.author
            );
            return;
        }

        if from_our_quorum && batch.author != self.name {
            warn!(
                "Ignoring local graph from quorum channel with mismatched author {}",
                batch.author
            );
            return;
        }

        let mut graphs_to_finalize = None;
        {
            let entry = self
                .sequences
                .entry(batch.sequence)
                .or_insert_with(SequenceState::default);

            let is_new_author = !entry.local_graphs.contains_key(&batch.author);
            if is_new_author {
                entry.collected_stake += author_stake;
                entry.local_graphs.insert(batch.author, batch.clone());
            }

            if from_our_quorum {
                entry.own_received = true;
            }

            let quorum = self.committee.quorum_threshold();
            if entry.own_received && entry.collected_stake >= quorum {
                graphs_to_finalize = Some(entry.local_graphs.values().cloned().collect::<Vec<_>>());
            }
        }

        if let Some(graphs) = graphs_to_finalize {
            let graph_refs: Vec<_> = graphs.iter().collect();
            let global_batch = self.build_global_batch(batch.sequence, graph_refs);
            let message = WorkerMessage::GlobalBatch(global_batch);
            let serialized = bincode::serialize(&message)
                .expect("Failed to serialize global-order graph as worker message");

            self.tx_global
                .send(serialized)
                .await
                .expect("Failed to send global-order graph");

            self.finalized.insert(batch.sequence);
            self.sequences.remove(&batch.sequence);
            debug!("Global order finalized for sequence {}", batch.sequence);
        }
    }

    fn build_global_batch(&self, sequence: u64, local_graphs: Vec<&Batch>) -> Batch {
        let quorum = self.committee.quorum_threshold();
        let validity = self.committee.validity_threshold();

        let mut support: HashMap<u64, Stake> = HashMap::new();
        let mut canonical_tx: HashMap<u64, Transaction> = HashMap::new();
        let mut state_key: HashMap<u64, u8> = HashMap::new();

        for graph in &local_graphs {
            let stake = self.committee.stake(&graph.author);
            let mut seen = HashSet::new();
            for tx in &graph.transactions {
                if let Some((tx_id, key)) = parse_transaction_id_and_state_key(tx) {
                    if seen.insert(tx_id) {
                        *support.entry(tx_id).or_insert(0) += stake;
                    }

                    match canonical_tx.get_mut(&tx_id) {
                        Some(current) => {
                            if tx.as_slice() < current.as_slice() {
                                *current = tx.clone();
                            }
                        }
                        None => {
                            canonical_tx.insert(tx_id, tx.clone());
                        }
                    }

                    state_key.entry(tx_id).or_insert(key);
                }
            }
        }

        let fixed_txs: HashSet<u64> = support
            .iter()
            .filter_map(|(tx_id, count)| (*count >= quorum).then_some(*tx_id))
            .collect();
        let pending_txs: HashSet<u64> = support
            .iter()
            .filter_map(|(tx_id, count)| (*count >= validity && *count < quorum).then_some(*tx_id))
            .collect();

        let mut nodes: HashSet<u64> = support
            .iter()
            .filter_map(|(tx_id, count)| (*count >= validity).then_some(*tx_id))
            .collect();

        let mut edges = HashSet::new();
        for graph in &local_graphs {
            for &(from, to) in &graph.edges {
                if nodes.contains(&from) && nodes.contains(&to) && from != to {
                    edges.insert((from, to));
                }
            }
        }

        // Re-introduce pending->pending edges that may be missing after a partial merge.
        let mut missing_edge_set = HashSet::new();
        for graph in &local_graphs {
            for &(from, to) in &graph.edges {
                if !nodes.contains(&from) || !nodes.contains(&to) {
                    continue;
                }

                if pending_txs.contains(&from)
                    && pending_txs.contains(&to)
                    && state_key.get(&from) == state_key.get(&to)
                    && !edges.contains(&(from, to))
                {
                    missing_edge_set.insert((from, to));
                }
            }
        }
        edges.extend(missing_edge_set);

        // Remove edges from pending transactions to fixed transactions.
        edges.retain(|(from, to)| !(pending_txs.contains(from) && fixed_txs.contains(to)));

        self.prune_cycles(&mut nodes, &mut edges, &fixed_txs, &pending_txs, &state_key);

        let reduced_edges = Self::transitive_reduction(&nodes, &edges);
        let ordered_tx_ids = Self::topological_sort(&nodes, &reduced_edges);

        let transactions = ordered_tx_ids
            .into_iter()
            .filter_map(|tx_id| canonical_tx.get(&tx_id).cloned())
            .collect();

        let mut final_edges: Vec<_> = reduced_edges
            .into_iter()
            .filter(|(from, to)| nodes.contains(from) && nodes.contains(to) && from != to)
            .collect();
        final_edges.sort_unstable();

        Batch {
            author: self.name,
            sequence,
            transactions,
            edges: final_edges,
        }
    }

    fn prune_cycles(
        &self,
        nodes: &mut HashSet<u64>,
        edges: &mut HashSet<(u64, u64)>,
        fixed_txs: &HashSet<u64>,
        pending_txs: &HashSet<u64>,
        state_key: &HashMap<u64, u8>,
    ) {
        loop {
            let sccs = Self::tarjan_scc(nodes, edges);
            let mut pruned_any = false;

            for scc in sccs {
                if scc.len() <= 1 {
                    continue;
                }

                let fixed_in_scc: Vec<u64> = scc
                    .iter()
                    .copied()
                    .filter(|tx_id| fixed_txs.contains(tx_id))
                    .collect();
                let pending_in_scc: Vec<u64> = scc
                    .iter()
                    .copied()
                    .filter(|tx_id| pending_txs.contains(tx_id))
                    .collect();

                let prune_fixed = pending_in_scc.len() >= fixed_in_scc.len();
                let mut prune = if prune_fixed {
                    fixed_in_scc.clone()
                } else {
                    pending_in_scc.clone()
                };
                let mut keep = if prune_fixed {
                    pending_in_scc
                } else {
                    fixed_in_scc
                };

                // Fallback: if one class is empty in the SCC, prune one deterministic node.
                if prune.is_empty() {
                    if let Some(victim) = scc.iter().copied().max() {
                        prune.push(victim);
                        keep = scc
                            .iter()
                            .copied()
                            .filter(|tx_id| tx_id != &victim)
                            .collect();
                    }
                }

                if prune.is_empty() {
                    continue;
                }

                let prune_set: HashSet<u64> = prune.into_iter().collect();
                let keep_set: HashSet<u64> = keep.into_iter().collect();
                let edge_snapshot: Vec<(u64, u64)> = edges.iter().copied().collect();
                let mut redirected_edges = Vec::new();

                for pruned in &prune_set {
                    let Some(pruned_state_key) = state_key.get(pruned) else {
                        continue;
                    };

                    let targets: Vec<u64> = keep_set
                        .iter()
                        .copied()
                        .filter(|tx_id| state_key.get(tx_id) == Some(pruned_state_key))
                        .collect();

                    if targets.is_empty() {
                        continue;
                    }

                    for (from, to) in &edge_snapshot {
                        if to != pruned || prune_set.contains(from) || !nodes.contains(from) {
                            continue;
                        }

                        for target in &targets {
                            if from != target {
                                redirected_edges.push((*from, *target));
                            }
                        }
                    }
                }

                for pruned in &prune_set {
                    nodes.remove(pruned);
                }

                edges.retain(|(from, to)| nodes.contains(from) && nodes.contains(to) && from != to);
                for (from, to) in redirected_edges {
                    if nodes.contains(&from) && nodes.contains(&to) && from != to {
                        edges.insert((from, to));
                    }
                }

                pruned_any = true;
            }

            if !pruned_any {
                break;
            }
        }
    }

    fn tarjan_scc(nodes: &HashSet<u64>, edges: &HashSet<(u64, u64)>) -> Vec<Vec<u64>> {
        fn strong_connect(
            node: u64,
            index: &mut usize,
            adjacency: &HashMap<u64, Vec<u64>>,
            indices: &mut HashMap<u64, usize>,
            lowlink: &mut HashMap<u64, usize>,
            stack: &mut Vec<u64>,
            on_stack: &mut HashSet<u64>,
            sccs: &mut Vec<Vec<u64>>,
        ) {
            indices.insert(node, *index);
            lowlink.insert(node, *index);
            *index += 1;

            stack.push(node);
            on_stack.insert(node);

            let neighbors = adjacency.get(&node).cloned().unwrap_or_default();
            for next in neighbors {
                if !indices.contains_key(&next) {
                    strong_connect(
                        next, index, adjacency, indices, lowlink, stack, on_stack, sccs,
                    );
                    let next_lowlink = *lowlink
                        .get(&next)
                        .expect("Tarjan lowlink is missing after DFS traversal");
                    let node_lowlink = lowlink
                        .get_mut(&node)
                        .expect("Tarjan lowlink is missing for current node");
                    *node_lowlink = (*node_lowlink).min(next_lowlink);
                } else if on_stack.contains(&next) {
                    let next_index = *indices
                        .get(&next)
                        .expect("Tarjan index is missing for back-edge node");
                    let node_lowlink = lowlink
                        .get_mut(&node)
                        .expect("Tarjan lowlink is missing while handling back-edge");
                    *node_lowlink = (*node_lowlink).min(next_index);
                }
            }

            if lowlink.get(&node) == indices.get(&node) {
                let mut scc = Vec::new();
                while let Some(x) = stack.pop() {
                    on_stack.remove(&x);
                    scc.push(x);
                    if x == node {
                        break;
                    }
                }
                scc.sort_unstable();
                sccs.push(scc);
            }
        }

        let mut adjacency: HashMap<u64, Vec<u64>> = HashMap::new();
        for &(from, to) in edges {
            if nodes.contains(&from) && nodes.contains(&to) {
                adjacency.entry(from).or_default().push(to);
            }
        }
        for neighbors in adjacency.values_mut() {
            neighbors.sort_unstable();
        }

        let mut sorted_nodes: Vec<_> = nodes.iter().copied().collect();
        sorted_nodes.sort_unstable();

        let mut index = 0usize;
        let mut indices = HashMap::new();
        let mut lowlink = HashMap::new();
        let mut stack = Vec::new();
        let mut on_stack = HashSet::new();
        let mut sccs = Vec::new();

        for node in sorted_nodes {
            if !indices.contains_key(&node) {
                strong_connect(
                    node,
                    &mut index,
                    &adjacency,
                    &mut indices,
                    &mut lowlink,
                    &mut stack,
                    &mut on_stack,
                    &mut sccs,
                );
            }
        }

        sccs
    }

    fn transitive_reduction(
        nodes: &HashSet<u64>,
        edges: &HashSet<(u64, u64)>,
    ) -> HashSet<(u64, u64)> {
        let mut reduced = edges.clone();
        let mut ordered_edges: Vec<_> = edges.iter().copied().collect();
        ordered_edges.sort_unstable();

        for (from, to) in ordered_edges {
            reduced.remove(&(from, to));
            if !Self::has_path(from, to, nodes, &reduced) {
                reduced.insert((from, to));
            }
        }

        reduced
    }

    fn has_path(
        start: u64,
        target: u64,
        nodes: &HashSet<u64>,
        edges: &HashSet<(u64, u64)>,
    ) -> bool {
        if start == target {
            return true;
        }

        let mut adjacency: HashMap<u64, Vec<u64>> = HashMap::new();
        for &(from, to) in edges {
            if nodes.contains(&from) && nodes.contains(&to) {
                adjacency.entry(from).or_default().push(to);
            }
        }
        for neighbors in adjacency.values_mut() {
            neighbors.sort_unstable();
        }

        let mut visited = HashSet::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            if !visited.insert(node) {
                continue;
            }

            if let Some(neighbors) = adjacency.get(&node) {
                for &next in neighbors.iter().rev() {
                    if next == target {
                        return true;
                    }
                    if !visited.contains(&next) {
                        stack.push(next);
                    }
                }
            }
        }

        false
    }

    fn topological_sort(nodes: &HashSet<u64>, edges: &HashSet<(u64, u64)>) -> Vec<u64> {
        let mut indegree: HashMap<u64, usize> = nodes.iter().map(|node| (*node, 0usize)).collect();
        let mut adjacency: HashMap<u64, Vec<u64>> = HashMap::new();

        for &(from, to) in edges {
            if nodes.contains(&from) && nodes.contains(&to) {
                adjacency.entry(from).or_default().push(to);
                *indegree
                    .get_mut(&to)
                    .expect("Topological sort node missing in indegree map") += 1;
            }
        }
        for neighbors in adjacency.values_mut() {
            neighbors.sort_unstable();
        }

        let mut ready = BTreeSet::new();
        for (node, degree) in &indegree {
            if *degree == 0 {
                ready.insert(*node);
            }
        }

        let mut ordered = Vec::with_capacity(nodes.len());
        while let Some(node) = ready.iter().next().copied() {
            ready.remove(&node);
            ordered.push(node);

            if let Some(neighbors) = adjacency.get(&node) {
                for next in neighbors {
                    let degree = indegree
                        .get_mut(next)
                        .expect("Topological sort found unknown adjacency node");
                    *degree -= 1;
                    if *degree == 0 {
                        ready.insert(*next);
                    }
                }
            }
        }

        if ordered.len() < nodes.len() {
            let ordered_set: HashSet<u64> = ordered.iter().copied().collect();
            let mut leftovers: Vec<_> = nodes
                .iter()
                .copied()
                .filter(|node| !ordered_set.contains(node))
                .collect();
            leftovers.sort_unstable();
            ordered.extend(leftovers);
        }

        ordered
    }
}
