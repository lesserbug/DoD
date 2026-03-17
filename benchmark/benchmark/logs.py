# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers =  len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, sent_samples = zip(*results)
        self.sent_samples = self._merge_results([x.items() for x in sent_samples])
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([x.items() for x in proposals])
        self.commits = self._merge_results([x.items() for x in commits])

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        (
            sizes,
            batch_times,
            received_samples,
            local_graph_times,
            local_graph_samples,
            executed_sizes,
            executed_times,
            executed_samples,
            executor_metrics,
            workers_ips,
        ) = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }
        self.batch_times = {
            k: v for k, v in self._merge_results([x.items() for x in batch_times]).items()
            if k in self.sizes
        }
        self.measured_proposals = {
            k: v for k, v in self.proposals.items() if k in self.sizes
        }
        self.measured_commits = {
            k: v for k, v in self.commits.items() if k in self.sizes
        }
        self.local_graph_times = self._merge_results([
            x.items() for x in local_graph_times
        ])
        # Keep execution metrics on the same canonical digest set used by the
        # commit-path throughput numbers. Otherwise, a canonical worker would
        # log execution for every committed digest with its worker id, which
        # inflates execution TPS above the user input rate.
        self.executed_sizes = {
            k: v for x in executed_sizes for k, v in x.items() if k in self.sizes
        }
        self.executed_times = self._merge_results([
            x.items() for x in executed_times
        ])
        self.measured_executions = {
            k: v for k, v in self.executed_times.items() if k in self.executed_sizes
        }
        self.received_samples = {}
        for samples in received_samples:
            for tx_id, batch_ids in samples.items():
                self.received_samples.setdefault(tx_id, set()).update(batch_ids)
        self.local_graph_samples = {}
        for samples in local_graph_samples:
            for tx_id, batch_ids in samples.items():
                self.local_graph_samples.setdefault(tx_id, set()).update(batch_ids)
        self.executed_samples = {}
        for samples in executed_samples:
            for tx_id, batch_ids in samples.items():
                self.executed_samples.setdefault(tx_id, set()).update(batch_ids)
        self.executor_metrics = {
            'same_batch_fallback_batches': max(
                (metrics['same_batch_fallback_batches'] for metrics in executor_metrics),
                default=0,
            ),
            'same_batch_fallback_pairs': max(
                (metrics['same_batch_fallback_pairs'] for metrics in executor_metrics),
                default=0,
            ),
            'dropped_observations': max(
                (metrics['dropped_observations'] for metrics in executor_metrics),
                default=0,
            ),
            'pending_health_events': max(
                (metrics['pending_health_events'] for metrics in executor_metrics),
                default=0,
            ),
            'processed_trim_blocked_events': max(
                (metrics['processed_trim_blocked_events'] for metrics in executor_metrics),
                default=0,
            ),
            'pending_batches': max(
                (metrics['pending_batches'] for metrics in executor_metrics),
                default=0,
            ),
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            'header_size': int(
                search(r'Header size .* (\d+)', log).group(1)
            ),
            'max_header_delay': int(
                search(r'Max header delay .* (\d+)', log).group(1)
            ),
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        def first_seen(entries):
            merged = {}
            for digest, timestamp in entries:
                if digest not in merged or merged[digest] > timestamp:
                    merged[digest] = timestamp
            return merged

        batch_entries = [
            (d, self._to_posix(t), int(s))
            for t, d, s in findall(
                r'\[(.*Z) .* (?<!\w)Batch ([^ ]+) contains (\d+) B',
                log,
            )
        ]
        sizes = {d: size for d, _, size in batch_entries}
        batch_times = first_seen([(d, timestamp) for d, timestamp, _ in batch_entries])

        tmp = findall(r'(?<!\w)Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {}
        for d, s in tmp:
            samples.setdefault(int(s), set()).add(d)

        local_entries = [
            (d, self._to_posix(t))
            for t, d in findall(
                r'\[(.*Z) .* LocalGraph ([^ ]+) contains \d+ B',
                log,
            )
        ]
        local_graph_times = first_seen(local_entries)
        tmp = findall(r'LocalGraph ([^ ]+) contains sample tx (\d+)', log)
        local_graph_samples = {}
        for d, s in tmp:
            local_graph_samples.setdefault(int(s), set()).add(d)

        executed_entries = [
            (d, self._to_posix(t), int(s))
            for t, d, s in findall(
                r'\[(.*Z) .* ExecutedBatch ([^ ]+) contains (\d+) B',
                log,
            )
        ]
        executed_sizes = {d: size for d, _, size in executed_entries}
        executed_times = first_seen(
            [(d, timestamp) for d, timestamp, _ in executed_entries]
        )
        tmp = findall(r'ExecutedBatch ([^ ]+) contains sample tx (\d+)', log)
        executed_samples = {}
        for d, s in tmp:
            executed_samples.setdefault(int(s), set()).add(d)

        metrics_entries = [
            tuple(map(int, entry))
            for entry in findall(
                r'ExecutorMetrics fallback_batches=(\d+) fallback_pairs=(\d+) dropped_observations=(\d+) pending_health_events=(\d+) processed_trim_blocked_events=(\d+) pending_batches=(\d+)',
                log,
            )
        ]
        if metrics_entries:
            (
                same_batch_fallback_batches,
                same_batch_fallback_pairs,
                dropped_observations,
                pending_health_events,
                processed_trim_blocked_events,
                pending_batches,
            ) = map(max, zip(*metrics_entries))
        else:
            same_batch_fallback_batches = 0
            same_batch_fallback_pairs = 0
            dropped_observations = 0
            pending_health_events = 0
            processed_trim_blocked_events = 0
            pending_batches = 0

        executor_metrics = {
            'same_batch_fallback_batches': same_batch_fallback_batches,
            'same_batch_fallback_pairs': same_batch_fallback_pairs,
            'dropped_observations': dropped_observations,
            'pending_health_events': pending_health_events,
            'processed_trim_blocked_events': processed_trim_blocked_events,
            'pending_batches': pending_batches,
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return (
            sizes,
            batch_times,
            samples,
            local_graph_times,
            local_graph_samples,
            executed_sizes,
            executed_times,
            executed_samples,
            executor_metrics,
            ip,
        )

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.measured_commits:
            return 0, 0, 0
        start, end = min(self.measured_proposals.values()), max(self.measured_commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.measured_proposals[d] for d, c in self.measured_commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.measured_commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.measured_commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        latency = []
        for tx_id, batch_ids in self.received_samples.items():
            if tx_id not in self.sent_samples:
                continue

            committed = [
                self.measured_commits[batch_id]
                for batch_id in batch_ids
                if batch_id in self.measured_commits
            ]
            if committed:
                start = self.sent_samples[tx_id]
                latency += [min(committed) - start]
        return mean(latency) if latency else 0

    def _execution_throughput(self):
        if not self.measured_executions:
            return 0, 0, 0
        start, end = min(self.start), max(self.measured_executions.values())
        duration = end - start
        bytes = sum(self.executed_sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _execution_latency(self):
        latency = []
        for tx_id, batch_ids in self.executed_samples.items():
            if tx_id not in self.sent_samples:
                continue

            executed = [
                self.measured_executions[batch_id]
                for batch_id in batch_ids
                if batch_id in self.measured_executions
            ]
            if executed:
                start = self.sent_samples[tx_id]
                latency += [min(executed) - start]
        return mean(latency) if latency else 0

    def _sample_timestamp(self, sample_map, digest_times, tx_id):
        timestamps = [
            digest_times[digest]
            for digest in sample_map.get(tx_id, set())
            if digest in digest_times
        ]
        return min(timestamps) if timestamps else None

    def _local_graph_latency(self):
        latency = []
        for tx_id, start in self.sent_samples.items():
            local = self._sample_timestamp(
                self.local_graph_samples,
                self.local_graph_times,
                tx_id,
            )
            if local is not None:
                latency += [local - start]
        return mean(latency) if latency else 0

    def _local_to_global_latency(self):
        latency = []
        for tx_id in self.sent_samples:
            local = self._sample_timestamp(
                self.local_graph_samples,
                self.local_graph_times,
                tx_id,
            )
            global_batch = self._sample_timestamp(
                self.received_samples,
                self.batch_times,
                tx_id,
            )
            if local is not None and global_batch is not None:
                latency += [global_batch - local]
        return mean(latency) if latency else 0

    def _global_to_commit_latency(self):
        latency = []
        for tx_id in self.sent_samples:
            global_batch = self._sample_timestamp(
                self.received_samples,
                self.batch_times,
                tx_id,
            )
            commit = self._sample_timestamp(
                self.received_samples,
                self.measured_commits,
                tx_id,
            )
            if global_batch is not None and commit is not None:
                latency += [commit - global_batch]
        return mean(latency) if latency else 0

    def _commit_to_execute_latency(self):
        latency = []
        for tx_id in self.sent_samples:
            commit = self._sample_timestamp(
                self.received_samples,
                self.measured_commits,
                tx_id,
            )
            execute = self._sample_timestamp(
                self.executed_samples,
                self.measured_executions,
                tx_id,
            )
            if commit is not None and execute is not None:
                latency += [execute - commit]
        return mean(latency) if latency else 0

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, commit_duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000
        execution_tps, execution_bps, execution_duration = self._execution_throughput()
        execution_latency = self._execution_latency() * 1_000
        local_graph_latency = self._local_graph_latency() * 1_000
        local_to_global_latency = self._local_to_global_latency() * 1_000
        global_to_commit_latency = self._global_to_commit_latency() * 1_000
        commit_to_execute_latency = self._commit_to_execute_latency() * 1_000
        same_batch_fallback_batches = self.executor_metrics['same_batch_fallback_batches']
        same_batch_fallback_pairs = self.executor_metrics['same_batch_fallback_pairs']
        dropped_observations = self.executor_metrics['dropped_observations']
        pending_health_events = self.executor_metrics['pending_health_events']
        processed_trim_blocked_events = self.executor_metrics['processed_trim_blocked_events']
        pending_batches = self.executor_metrics['pending_batches']

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Commit path time: {round(commit_duration):,} s\n'
            f' Execution path time: {round(execution_duration):,} s\n'
            '\n'
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '\n'
            f' Execution TPS: {round(execution_tps):,} tx/s\n'
            f' Execution BPS: {round(execution_bps):,} B/s\n'
            f' Execution latency: {round(execution_latency):,} ms\n'
            '\n'
            ' + STAGES:\n'
            f' Client -> local graph: {round(local_graph_latency):,} ms\n'
            f' Local graph -> global graph: {round(local_to_global_latency):,} ms\n'
            f' Global graph -> commit: {round(global_to_commit_latency):,} ms\n'
            f' Commit -> execute: {round(commit_to_execute_latency):,} ms\n'
            '\n'
            ' + EXECUTOR:\n'
            f' Same-batch fallback batches: {same_batch_fallback_batches:,}\n'
            f' Same-batch fallback pairs: {same_batch_fallback_pairs:,}\n'
            f' Dropped global-graph observations: {dropped_observations:,}\n'
            f' Pending queue health events: {pending_health_events:,}\n'
            f' Processed trim blocked events: {processed_trim_blocked_events:,}\n'
            f' Pending batches (max snapshot): {pending_batches:,}\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'a') as f:
            f.write(self.result())

    @classmethod
    def process(cls, directory, faults=0):
        assert isinstance(directory, str)

        clients = []
        for filename in sorted(glob(join(directory, 'client-*.log'))):
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in sorted(glob(join(directory, 'primary-*.log'))):
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in sorted(glob(join(directory, 'worker-*.log'))):
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
