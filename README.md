# DoD-Style Graph-Ordering Reference

This repository contains a DoD-style graph-ordering reference implemented in the same Narwhal/Tusk-based experimental harness used for MRV.

The implementation is included to provide a nearby DAG-BFT order-fairness design point for comparison: unlike MRV, which derives structural ordering constraints after consensus from the committed DAG, the DoD-style reference carries explicit graph-based ordering information through the system pipeline.

## Scope

This is an independent reference implementation based on the design described in the DoD paper. It is not the official DoD implementation, and it should not be interpreted as a reproduction of DoD's published artifact or performance numbers.

The original DoD code was not publicly available to us at the time of implementation. We therefore implemented the relevant graph-ordering workflow inside our Narwhal/Tusk benchmark harness to compare the system-level cost of explicit graph-based ordering against MRV's post-consensus interpretation approach.

## What Is Implemented

At a high level, this reference follows the DoD-style pipeline:

1. Workers disseminate local-order graph information through the Narwhal/Tusk mempool path.
2. The system derives a global-order graph locally.
3. The graph is processed in the role normally played by a transaction batch in the Tusk execution pipeline.
4. End-to-end throughput and latency are measured using the same benchmark harness as the MRV experiments.

## Important Differences from Published DoD Results

This implementation is intended as a controlled reference point within our experimental harness, not as an apples-to-apples reproduction of the DoD evaluation.

Key differences include:

- **Implementation**: this is our independent DoD-style implementation, not the original authors' code.
- **Deployment**: our experiments use the same AWS-based Narwhal/Tusk benchmark environment as MRV, which differs from the CloudLab bare-metal environment used in DoD's published evaluation.
- **Workload structure**: DoD's reported performance benefits from data-dependent fairness, where ordering constraints are added mainly between dependent transaction pairs. Workloads with sparse dependencies produce much sparser graphs than workloads where most transactions are mutually dependent.
- **Purpose**: the goal is to compare design points inside one harness: explicit graph-ordering in the pipeline versus MRV's post-consensus structural interpretation.

For these reasons, the throughput and latency numbers reported for this reference should be interpreted as measurements of our DoD-style design point under our benchmark setting, rather than as claims about the original DoD implementation.

## Metrics

The benchmark output reports several metrics:

- **Consensus TPS / latency:** throughput and latency of the consensus commit path.
- **End-to-end TPS / latency:** client-perceived throughput and latency up to commit.
- **Execution TPS / latency:** throughput and latency up to execution of globally ordered graph batches.

For experiments that evaluate complete transaction ordering and confirmation, we use **Execution TPS** and **Execution latency** as the primary DoD-style metrics. The stage breakdown is also useful for diagnosing where time is spent:

- `Client -> local graph`
- `Local graph -> global graph`
- `Global graph -> commit`
- `Commit -> execute`

## Running Experiments

Use the benchmark scripts in the repository's `benchmark/` directory. The workflow follows the same local and remote benchmark structure used by the MRV artifact.

Typical local run:

```bash
cd benchmark
pip install -r requirements.txt
fab local
```

For AWS experiments, edit `benchmark/settings.json` with the appropriate repository, branch, SSH key, instance type, and regions, then use:

```bash
fab create --nodes=1
fab install
fab remote
fab stop
```

Use `fab destroy` when the AWS testbed is no longer needed.
