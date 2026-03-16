// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
#[cfg(feature = "benchmark")]
use crate::worker::WorkerMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
#[cfg(feature = "benchmark")]
use log::info;
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
        // Whether this processor should emit benchmark size/sample logs.
        benchmark_log_batches: bool,
    ) {
        tokio::spawn(async move {
            #[cfg(not(feature = "benchmark"))]
            let _ = benchmark_log_batches;

            while let Some(batch) = rx_batch.recv().await {
                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());

                #[cfg(feature = "benchmark")]
                if own_digest && benchmark_log_batches {
                    if let Ok(WorkerMessage::GlobalBatch(global)) =
                        bincode::deserialize::<WorkerMessage>(&batch)
                    {
                        let size: usize = global.transactions.iter().map(|tx| tx.len()).sum();
                        for tx in &global.transactions {
                            if tx.first() == Some(&0u8) && tx.len() > 8 {
                                let id_bytes: [u8; 8] = tx[1..9]
                                    .try_into()
                                    .expect("sample transaction id should be 8 bytes");
                                info!(
                                    "Batch {:?} contains sample tx {}",
                                    digest,
                                    u64::from_be_bytes(id_bytes)
                                );
                            }
                        }

                        info!("Batch {:?} contains {} B", digest, size);
                    }
                }

                // Store the batch.
                store.write(digest.to_vec(), batch).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
