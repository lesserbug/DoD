// Copyright(C) Facebook, Inc. and its affiliates.
use crate::batch_maker::{
    parse_standard_transaction, parse_transaction_id_and_state_key, Batch, Transaction,
};
use crate::processor::SerializedBatchMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use config::{Committee, Stake};
use crypto::PublicKey;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, warn};
use network::ReliableSender;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{Duration, Instant};

#[cfg(test)]
#[path = "tests/global_orderer_tests.rs"]
pub mod global_orderer_tests;

#[derive(Default)]
struct SequenceState {
    own_received: bool,
    collected_stake: Stake,
    local_graphs: HashMap<PublicKey, Batch>,
    quorum_reached_at: Option<Instant>,
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
    /// The network addresses of other workers sharing our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// A network sender to disseminate global-order graphs.
    network: ReliableSender,
    /// Per-sequence collection state.
    sequences: HashMap<u64, SequenceState>,
    /// Finalized sequences are ignored if duplicated later.
    finalized: HashSet<u64>,
}

impl GlobalOrderer {
    const STABILIZATION_DELAY_MS: u64 = 10;
    const ORDER_FAIRNESS_GAMMA_NUM: Stake = 1;
    const ORDER_FAIRNESS_GAMMA_DEN: Stake = 1;

    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        rx_own_local: Receiver<SerializedBatchMessage>,
        rx_workers_local: Receiver<SerializedBatchMessage>,
        tx_global: Sender<SerializedBatchMessage>,
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                rx_own_local,
                rx_workers_local,
                tx_global,
                workers_addresses,
                network: ReliableSender::new(),
                sequences: HashMap::new(),
                finalized: HashSet::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut stabilization_tick =
            tokio::time::interval(Duration::from_millis(Self::STABILIZATION_DELAY_MS));

        loop {
            tokio::select! {
                Some(serialized) = self.rx_own_local.recv() => {
                    self.handle_local_graph(serialized, true).await;
                }
                Some(serialized) = self.rx_workers_local.recv() => {
                    self.handle_local_graph(serialized, false).await;
                }
                _ = stabilization_tick.tick() => {}
                else => {
                    break;
                }
            }

            self.try_finalize_ready_sequences().await;
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

        let quorum = self.committee.quorum_threshold();
        let entry = self
            .sequences
            .entry(batch.sequence)
            .or_insert_with(SequenceState::default);

        let is_new_author = !entry.local_graphs.contains_key(&batch.author);
        if is_new_author {
            entry.collected_stake += author_stake;
            entry.local_graphs.insert(batch.author, batch);
        }

        if from_our_quorum {
            entry.own_received = true;
        }

        if entry.own_received
            && entry.collected_stake >= quorum
            && entry.quorum_reached_at.is_none()
        {
            entry.quorum_reached_at = Some(Instant::now());
        }
    }

    async fn try_finalize_ready_sequences(&mut self) {
        let ready_sequences = self.ready_sequences(Instant::now());
        for sequence in ready_sequences {
            let graphs = match self.sequences.get(&sequence) {
                Some(state) => Self::select_quorum_graphs(&self.committee, &state.local_graphs),
                None => continue,
            };

            if graphs.is_empty() {
                continue;
            }

            if self.finalize_sequence(sequence, graphs).await {
                self.finalized.insert(sequence);
                self.sequences.remove(&sequence);
                debug!("Global order finalized for sequence {}", sequence);
            }
        }
    }

    fn ready_sequences(&self, now: Instant) -> Vec<u64> {
        let delay = Duration::from_millis(Self::STABILIZATION_DELAY_MS);
        let quorum = self.committee.quorum_threshold();
        let mut ready = Vec::new();

        for (&sequence, state) in &self.sequences {
            if self.finalized.contains(&sequence)
                || !state.own_received
                || state.collected_stake < quorum
            {
                continue;
            }

            if let Some(reached_at) = state.quorum_reached_at {
                if now.duration_since(reached_at) >= delay {
                    ready.push(sequence);
                }
            }
        }

        ready.sort_unstable();
        ready
    }

    fn select_quorum_graphs(
        committee: &Committee,
        local_graphs: &HashMap<PublicKey, Batch>,
    ) -> Vec<Batch> {
        let mut sorted_graphs: Vec<_> = local_graphs
            .iter()
            .map(|(author, batch)| (*author, batch.clone()))
            .collect();
        sorted_graphs.sort_unstable_by_key(|(author, _)| *author);

        let quorum = committee.quorum_threshold();
        let mut selected = Vec::new();
        let mut total_stake = 0;
        for (author, batch) in sorted_graphs {
            total_stake += committee.stake(&author);
            selected.push(batch);
            if total_stake >= quorum {
                return selected;
            }
        }

        Vec::new()
    }

    async fn finalize_sequence(&mut self, sequence: u64, graphs: Vec<Batch>) -> bool {
        let name = self.name;
        let committee = self.committee.clone();
        let global_batch = tokio::task::spawn_blocking(move || {
            Self::build_global_batch(name, committee, sequence, graphs)
        })
        .await
        .expect("Global orderer task panicked while building global-order graph");
        let message = WorkerMessage::GlobalBatch(global_batch);
        let serialized = bincode::serialize(&message)
            .expect("Failed to serialize global-order graph as worker message");

        // Disseminate the global-order graph to peer workers.
        let (names, addresses): (Vec<_>, Vec<_>) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        let handlers = self.network.broadcast(addresses, bytes).await;

        // Mimic Narwhal's batch path: only forward to primary once n-f workers acknowledged.
        let reached_quorum = if cfg!(test) || names.is_empty() {
            // Unit tests may run without a network topology.
            true
        } else {
            let mut wait_for_quorum: FuturesUnordered<_> = names
                .into_iter()
                .zip(handlers.into_iter())
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    async move {
                        let _ = handler.await;
                        stake
                    }
                })
                .collect();

            let mut total_stake = self.committee.stake(&self.name);
            let quorum = self.committee.quorum_threshold();
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                if total_stake >= quorum {
                    break;
                }
            }
            total_stake >= quorum
        };
        if !reached_quorum {
            warn!(
                "Global graph sequence {} did not reach quorum acknowledgements",
                sequence
            );
            return false;
        }

        // Deliver locally for hashing/storage and primary notification.
        self.tx_global
            .send(serialized)
            .await
            .expect("Failed to send global-order graph");

        true
    }

    fn build_global_batch(
        name: PublicKey,
        committee: Committee,
        sequence: u64,
        local_graphs: Vec<Batch>,
    ) -> Batch {
        let fixed_threshold = Self::fixed_threshold(&committee);
        let pending_threshold = Self::pending_threshold(&committee);

        let mut support: HashMap<u64, Stake> = HashMap::new();
        let mut canonical_tx: HashMap<u64, Transaction> = HashMap::new();
        let mut state_key: HashMap<u64, u8> = HashMap::new();

        for graph in &local_graphs {
            let stake = committee.stake(&graph.author);
            let mut seen = HashSet::new();
            for tx in &graph.transactions {
                if let Some((tx_id, _key)) = parse_transaction_id_and_state_key(tx) {
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

                    if let Some((_, standard_key)) = parse_standard_transaction(tx) {
                        state_key.entry(tx_id).or_insert(standard_key);
                    }
                }
            }
        }

        let fixed_txs: HashSet<u64> = support
            .iter()
            .filter_map(|(tx_id, count)| (*count >= fixed_threshold).then_some(*tx_id))
            .collect();
        let pending_txs: HashSet<u64> = support
            .iter()
            .filter_map(|(tx_id, count)| {
                (*count >= pending_threshold && *count < fixed_threshold).then_some(*tx_id)
            })
            .collect();

        let mut nodes: HashSet<u64> = support
            .iter()
            .filter_map(|(tx_id, count)| (*count >= pending_threshold).then_some(*tx_id))
            .collect();

        let edge_weights = Self::collect_edge_weights(&committee, &local_graphs, &nodes);
        let mut edges =
            Self::build_weighted_edges(&nodes, &state_key, &edge_weights, pending_threshold);
        Self::retain_sccs_with_path_to_fixed(&mut nodes, &mut edges, &fixed_txs, &pending_txs);
        let forwarded_in_batch_support =
            Self::collect_forwarded_in_batch_support(&committee, &local_graphs, &nodes);
        let sccs = Self::tarjan_scc(&nodes, &edges);
        let component_index = Self::component_index(&sccs);
        let forwarded_missing_edges = Self::collect_forwarded_external_missing_edges(
            &committee,
            &local_graphs,
            &nodes,
            pending_threshold,
        );
        Self::linearize_sccs(&mut edges, &sccs);

        let reduced_edges = Self::transitive_reduction(&nodes, &edges);
        let ordered_tx_ids = Self::topological_sort(&nodes, &reduced_edges);
        let promoted_in_batch_edges = Self::collect_frontier_promoted_in_batch_edges(
            &ordered_tx_ids,
            &nodes,
            &reduced_edges,
            &state_key,
            &forwarded_in_batch_support,
            pending_threshold,
        );
        let mut final_edge_set = reduced_edges.clone();
        final_edge_set.extend(promoted_in_batch_edges);
        let missing_edges = Self::collect_missing_edges(
            &ordered_tx_ids,
            &nodes,
            &final_edge_set,
            &component_index,
            &state_key,
        );

        let transactions = ordered_tx_ids
            .into_iter()
            .filter_map(|tx_id| canonical_tx.get(&tx_id).cloned())
            .collect();

        let mut final_edges: Vec<_> = final_edge_set
            .into_iter()
            .filter(|(from, to)| nodes.contains(from) && nodes.contains(to) && from != to)
            .collect();
        final_edges.sort_unstable();
        let mut final_missing_edges: Vec<_> = missing_edges
            .into_iter()
            .filter(|(from, to)| nodes.contains(from) && nodes.contains(to) && from != to)
            .collect();
        final_missing_edges.extend(
            forwarded_missing_edges
                .into_iter()
                .filter(|(from, to)| (nodes.contains(from) || nodes.contains(to)) && from != to),
        );
        final_missing_edges.sort_unstable();
        final_missing_edges.dedup();

        Batch {
            author: name,
            sequence,
            transactions,
            edges: final_edges,
            missing_edges: final_missing_edges,
        }
    }

    fn tolerated_faults(committee: &Committee) -> Stake {
        ((committee.size().saturating_sub(1)) / 3) as Stake
    }

    fn fixed_threshold(committee: &Committee) -> Stake {
        let replicas = committee.size() as Stake;
        let faults = Self::tolerated_faults(committee);
        replicas.saturating_sub(2 * faults).max(1)
    }

    fn pending_threshold(committee: &Committee) -> Stake {
        let replicas = committee.size() as Stake;
        let faults = Self::tolerated_faults(committee);
        let gamma_num = Self::ORDER_FAIRNESS_GAMMA_NUM.min(Self::ORDER_FAIRNESS_GAMMA_DEN);
        let gamma_den = Self::ORDER_FAIRNESS_GAMMA_DEN.max(1);
        let floor_n_times_one_minus_gamma =
            replicas.saturating_mul(gamma_den - gamma_num) / gamma_den;
        floor_n_times_one_minus_gamma
            .saturating_add(faults)
            .saturating_add(1)
            .max(1)
    }

    fn collect_edge_weights(
        committee: &Committee,
        local_graphs: &[Batch],
        nodes: &HashSet<u64>,
    ) -> HashMap<(u64, u64), Stake> {
        let mut edge_weights: HashMap<(u64, u64), Stake> = HashMap::new();

        for graph in local_graphs {
            let graph_stake = committee.stake(&graph.author);
            let mut seen_edges = HashSet::new();
            for &(from, to) in &graph.edges {
                if nodes.contains(&from)
                    && nodes.contains(&to)
                    && from != to
                    && seen_edges.insert((from, to))
                {
                    *edge_weights.entry((from, to)).or_insert(0) += graph_stake;
                }
            }
        }

        edge_weights
    }

    fn build_weighted_edges(
        nodes: &HashSet<u64>,
        state_key: &HashMap<u64, u8>,
        edge_weights: &HashMap<(u64, u64), Stake>,
        threshold: Stake,
    ) -> HashSet<(u64, u64)> {
        let mut txs_by_key: HashMap<u8, Vec<u64>> = HashMap::new();

        for &tx_id in nodes {
            if let Some(&key) = state_key.get(&tx_id) {
                txs_by_key.entry(key).or_default().push(tx_id);
            }
        }
        for tx_ids in txs_by_key.values_mut() {
            tx_ids.sort_unstable();
        }

        let mut edges = HashSet::new();
        for tx_ids in txs_by_key.values() {
            for (index, &left) in tx_ids.iter().enumerate() {
                for &right in tx_ids.iter().skip(index + 1) {
                    let forward = edge_weights.get(&(left, right)).copied().unwrap_or(0);
                    let backward = edge_weights.get(&(right, left)).copied().unwrap_or(0);

                    if forward >= threshold && forward > backward {
                        edges.insert((left, right));
                    } else if backward >= threshold && backward > forward {
                        edges.insert((right, left));
                    }
                }
            }
        }

        edges
    }

    fn retain_sccs_with_path_to_fixed(
        nodes: &mut HashSet<u64>,
        edges: &mut HashSet<(u64, u64)>,
        fixed_txs: &HashSet<u64>,
        pending_txs: &HashSet<u64>,
    ) {
        let sccs = Self::tarjan_scc(nodes, edges);
        if sccs.is_empty() {
            return;
        }

        let component_index = Self::component_index(&sccs);
        let mut reverse_edges: HashMap<usize, BTreeSet<usize>> = HashMap::new();
        let mut fixed_components = HashSet::new();
        let mut pending_components = HashSet::new();

        for (component, scc) in sccs.iter().enumerate() {
            if scc.iter().any(|tx_id| fixed_txs.contains(tx_id)) {
                fixed_components.insert(component);
            } else if scc.iter().any(|tx_id| pending_txs.contains(tx_id)) {
                pending_components.insert(component);
            }
        }

        if fixed_components.is_empty() {
            return;
        }

        for &(from, to) in edges.iter() {
            let Some(&from_component) = component_index.get(&from) else {
                continue;
            };
            let Some(&to_component) = component_index.get(&to) else {
                continue;
            };
            if from_component != to_component {
                reverse_edges
                    .entry(to_component)
                    .or_default()
                    .insert(from_component);
            }
        }

        let mut keep_components = fixed_components.clone();
        let mut stack: Vec<_> = fixed_components.into_iter().collect();
        while let Some(component) = stack.pop() {
            if let Some(previous) = reverse_edges.get(&component) {
                for &candidate in previous {
                    if pending_components.contains(&candidate) && keep_components.insert(candidate)
                    {
                        stack.push(candidate);
                    }
                }
            }
        }

        let keep_nodes: HashSet<u64> = keep_components
            .into_iter()
            .flat_map(|component| sccs[component].iter().copied())
            .collect();
        nodes.retain(|tx_id| keep_nodes.contains(tx_id));
        edges.retain(|(from, to)| nodes.contains(from) && nodes.contains(to) && from != to);
    }

    fn component_index(sccs: &[Vec<u64>]) -> HashMap<u64, usize> {
        let mut component_index = HashMap::new();
        for (component, scc) in sccs.iter().enumerate() {
            for &tx_id in scc {
                component_index.insert(tx_id, component);
            }
        }
        component_index
    }

    fn collect_missing_edges(
        ordered_tx_ids: &[u64],
        nodes: &HashSet<u64>,
        edges: &HashSet<(u64, u64)>,
        component_index: &HashMap<u64, usize>,
        state_key: &HashMap<u64, u8>,
    ) -> HashSet<(u64, u64)> {
        let adjacency = Self::build_adjacency(nodes, edges);
        let mut txs_by_key: HashMap<u8, Vec<u64>> = HashMap::new();
        for &tx_id in ordered_tx_ids {
            if nodes.contains(&tx_id) {
                if let Some(&key) = state_key.get(&tx_id) {
                    txs_by_key.entry(key).or_default().push(tx_id);
                }
            }
        }

        let mut missing_edges = HashSet::new();
        for tx_ids in txs_by_key.values() {
            let mut frontier = None;
            for &current in tx_ids {
                let Some(previous) = frontier else {
                    frontier = Some(current);
                    continue;
                };

                if component_index.get(&previous) == component_index.get(&current)
                    || Self::has_path_in_adjacency(previous, current, &adjacency)
                    || Self::has_path_in_adjacency(current, previous, &adjacency)
                {
                    frontier = Some(current);
                    continue;
                }

                missing_edges.insert((previous, current));
                frontier = Some(current);
            }
        }

        missing_edges
    }

    fn collect_forwarded_in_batch_support(
        committee: &Committee,
        local_graphs: &[Batch],
        nodes: &HashSet<u64>,
    ) -> HashMap<(u64, u64), Stake> {
        let mut weights: HashMap<(u64, u64), Stake> = HashMap::new();

        for graph in local_graphs {
            let graph_stake = committee.stake(&graph.author);
            let mut seen = HashSet::new();
            for &(from, to) in &graph.missing_edges {
                if from == to || !nodes.contains(&from) || !nodes.contains(&to) {
                    continue;
                }

                if seen.insert((from, to)) {
                    *weights.entry((from, to)).or_insert(0) += graph_stake;
                }
            }
        }

        weights
    }

    fn collect_frontier_promoted_in_batch_edges(
        ordered_tx_ids: &[u64],
        nodes: &HashSet<u64>,
        edges: &HashSet<(u64, u64)>,
        state_key: &HashMap<u64, u8>,
        support: &HashMap<(u64, u64), Stake>,
        threshold: Stake,
    ) -> HashSet<(u64, u64)> {
        let mut adjacency = Self::build_adjacency(nodes, edges);
        let mut txs_by_key: HashMap<u8, Vec<u64>> = HashMap::new();
        for &tx_id in ordered_tx_ids {
            if nodes.contains(&tx_id) {
                if let Some(&key) = state_key.get(&tx_id) {
                    txs_by_key.entry(key).or_default().push(tx_id);
                }
            }
        }

        let mut promoted = HashSet::new();
        for tx_ids in txs_by_key.values() {
            for (index, &current) in tx_ids.iter().enumerate() {
                for &candidate in tx_ids[..index].iter().rev() {
                    let forward_support = support.get(&(candidate, current)).copied().unwrap_or(0);
                    if forward_support < threshold {
                        continue;
                    }

                    let reverse_support = support.get(&(current, candidate)).copied().unwrap_or(0);
                    if forward_support <= reverse_support
                        || Self::has_path_in_adjacency(candidate, current, &adjacency)
                    {
                        continue;
                    }

                    promoted.insert((candidate, current));
                    adjacency.entry(candidate).or_default().insert(current);
                    break;
                }
            }
        }

        promoted
    }

    fn collect_forwarded_external_missing_edges(
        committee: &Committee,
        local_graphs: &[Batch],
        nodes: &HashSet<u64>,
        threshold: Stake,
    ) -> HashSet<(u64, u64)> {
        let mut weights: HashMap<(u64, u64), Stake> = HashMap::new();

        for graph in local_graphs {
            let graph_stake = committee.stake(&graph.author);
            let mut seen = HashSet::new();
            for &(from, to) in &graph.missing_edges {
                let pair = if from <= to { (from, to) } else { (to, from) };
                let left_in = nodes.contains(&pair.0);
                let right_in = nodes.contains(&pair.1);
                if pair.0 == pair.1 || left_in == right_in {
                    continue;
                }

                if seen.insert(pair) {
                    *weights.entry(pair).or_insert(0) += graph_stake;
                }
            }
        }

        weights
            .into_iter()
            .filter_map(|(pair, support)| (support >= threshold).then_some(pair))
            .collect()
    }

    fn linearize_sccs(edges: &mut HashSet<(u64, u64)>, sccs: &[Vec<u64>]) {
        for scc in sccs {
            if scc.len() <= 1 {
                continue;
            }

            let members: HashSet<u64> = scc.iter().copied().collect();
            edges.retain(|(from, to)| !(members.contains(from) && members.contains(to)));

            let mut ordered = scc.clone();
            ordered.sort_unstable();
            for window in ordered.windows(2) {
                edges.insert((window[0], window[1]));
            }
        }
    }

    #[allow(dead_code)]
    fn prune_cycles(
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
        let mut adjacency = Self::build_adjacency(nodes, &reduced);
        let mut ordered_edges: Vec<_> = edges.iter().copied().collect();
        ordered_edges.sort_unstable();

        for (from, to) in ordered_edges {
            if !reduced.remove(&(from, to)) {
                continue;
            }

            let mut remove_entry = false;
            if let Some(neighbors) = adjacency.get_mut(&from) {
                neighbors.remove(&to);
                remove_entry = neighbors.is_empty();
            }
            if remove_entry {
                adjacency.remove(&from);
            }

            if !Self::has_path_in_adjacency(from, to, &adjacency) {
                reduced.insert((from, to));
                adjacency.entry(from).or_default().insert(to);
            }
        }

        reduced
    }

    fn build_adjacency(
        nodes: &HashSet<u64>,
        edges: &HashSet<(u64, u64)>,
    ) -> HashMap<u64, BTreeSet<u64>> {
        let mut adjacency: HashMap<u64, BTreeSet<u64>> = HashMap::new();
        for &(from, to) in edges {
            if nodes.contains(&from) && nodes.contains(&to) {
                adjacency.entry(from).or_default().insert(to);
            }
        }
        adjacency
    }

    fn has_path_in_adjacency(
        start: u64,
        target: u64,
        adjacency: &HashMap<u64, BTreeSet<u64>>,
    ) -> bool {
        if start == target {
            return true;
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
