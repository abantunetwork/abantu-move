// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    read_manifest, FileMetadata, FileType, Manifest, CHECKPOINT_FILE_MAGIC, EPOCH_DIR_PREFIX,
    SUMMARY_FILE_MAGIC,
};
use anyhow::{anyhow, Context, Result};
use backoff::future::retry;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use futures::StreamExt;
use futures::TryStreamExt;
use integer_encoding::VarIntReader;
use object_store::path::Path;
use object_store::DynObjectStore;
use std::future;
use std::io::Read;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use sui_storage::object_store::ObjectStoreConfig;
use sui_storage::{verify_checkpoint, Blob, Encoding};
use sui_types::messages_checkpoint::{
    CertifiedCheckpointSummary as Checkpoint, CheckpointSequenceNumber,
    FullCheckpointContents as CheckpointContents, VerifiedCheckpointContents,
};
use sui_types::storage::{ReadStore, WriteStore};
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tracing::info;

pub struct ArchiveReaderV1 {
    remote_object_store: Arc<DynObjectStore>,
    local_object_store: Arc<DynObjectStore>,
    concurrency: usize,
    manifest: Arc<Mutex<Manifest>>,
    sender: Sender<()>,
}

impl ArchiveReaderV1 {
    pub async fn new(
        remote_store_config: &ObjectStoreConfig,
        local_store_config: &ObjectStoreConfig,
        download_concurrency: NonZeroUsize,
    ) -> Result<Self> {
        let remote_object_store = remote_store_config.make()?;

        let local_staging_dir_root = local_store_config
            .directory
            .as_ref()
            .context("Directory cannot be missing in local store config")?
            .clone();
        let local_object_store = local_store_config.make()?;

        let (sender, mut recv) = tokio::sync::oneshot::channel();
        let manifest = Arc::new(Mutex::new(Manifest::new(0, 0)));

        let cloned_local_store = local_object_store.clone();
        let cloned_remote_store = remote_object_store.clone();
        let cloned_manifest = manifest.clone();
        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let manifest = read_manifest(local_staging_dir_root.clone(), cloned_local_store.clone(), cloned_remote_store.clone()).await?;
                        let mut locked = cloned_manifest.lock().await;
                        *locked = manifest;
                    }
                    _ = &mut recv => break,
                }
            }
            info!("Terminating the manifest sync loop");
            Ok::<(), anyhow::Error>(())
        });
        Ok(ArchiveReaderV1 {
            remote_object_store,
            local_object_store,
            concurrency: download_concurrency.get(),
            manifest,
            sender,
        })
    }
    pub async fn read<S>(
        &mut self,
        store: S,
        checkpoint_range: Range<CheckpointSequenceNumber>,
    ) -> Result<()>
    where
        S: WriteStore + Clone,
        <S as ReadStore>::Error: std::error::Error,
    {
        let manifest = self.manifest.lock().await.clone();
        let files = manifest.files();
        if files.is_empty() {
            return Err(anyhow!("No files in archive store to read from"));
        }
        let mut summary_files: Vec<_> = files
            .clone()
            .into_iter()
            .filter(|f| f.file_type == FileType::CheckpointSummary)
            .collect();
        let mut contents_files: Vec<_> = files
            .into_iter()
            .filter(|f| f.file_type == FileType::CheckpointContent)
            .collect();
        assert_eq!(summary_files.len(), contents_files.len());

        summary_files.sort_by_key(|f| f.checkpoint_seq_range.start);
        contents_files.sort_by_key(|f| f.checkpoint_seq_range.start);

        let files: Vec<_> = summary_files
            .into_iter()
            .zip(contents_files.into_iter())
            .map(|(s, c)| {
                assert_eq!(s.checkpoint_seq_range, c.checkpoint_seq_range);
                (s, c)
            })
            .collect();

        let earliest_available_checkpoint = files.first().unwrap().0.checkpoint_seq_range.start;
        if checkpoint_range.start < earliest_available_checkpoint {
            return Err(anyhow!(
                "Archive cannot complete the request as the earliest checkpoint available is: {}",
                earliest_available_checkpoint
            ));
        }

        let latest_available_checkpoint = files.last().unwrap().0.checkpoint_seq_range.end;
        if checkpoint_range.start >= latest_available_checkpoint {
            return Err(anyhow!("Archive cannot complete the request as the latest available checkpoint available is: {}", latest_available_checkpoint));
        }

        let start_index = match files.binary_search_by_key(&checkpoint_range.start, |(s, _c)| {
            s.checkpoint_seq_range.start
        }) {
            Ok(index) => index,
            Err(index) => index.saturating_sub(1),
        };

        let end_index = match files.binary_search_by_key(&checkpoint_range.end, |(s, _c)| {
            s.checkpoint_seq_range.start
        }) {
            Ok(index) => index,
            Err(index) => index,
        };

        let remote_object_store = self.remote_object_store.clone();

        let results: Vec<Result<(), anyhow::Error>> = futures::stream::iter(files.iter())
            .enumerate()
            .filter(|(index, (_s, _c))| future::ready(*index >= start_index && *index < end_index))
            .map(|(_, (s, c))| {
                let backoff = backoff::ExponentialBackoff::default();
                let summary_file_path = s.file_path(&Self::epoch_dir(s.epoch_num));
                let checkpoint_file_path = c.file_path(&Self::epoch_dir(s.epoch_num));
                let remote_object_store = remote_object_store.clone();
                async move {
                    let summary_bytes = retry(backoff.clone(), || async {
                        remote_object_store
                            .get(&summary_file_path)
                            .await
                            .map_err(|e| anyhow!("Failed to download summary file: {e}"))
                            .map_err(backoff::Error::transient)
                    })
                    .await?
                    .bytes()
                    .await?;
                    let content_bytes = retry(backoff, || async {
                        remote_object_store
                            .get(&checkpoint_file_path)
                            .await
                            .map_err(|e| anyhow!("Failed to download contents file: {e}"))
                            .map_err(backoff::Error::transient)
                    })
                    .await?
                    .bytes()
                    .await?;
                    Ok::<((FileMetadata, Bytes), (FileMetadata, Bytes)), anyhow::Error>((
                        (s.clone(), summary_bytes),
                        (c.clone(), content_bytes),
                    ))
                }
            })
            .boxed()
            .buffered(self.concurrency)
            .map_ok(|((s, summary_bytes), (c, content_bytes))| {
                let summary_iter = CheckpointSummaryIter::new(&s, summary_bytes)
                    .expect("Checkpoint summary iter creation must not fail");
                let content_iter = CheckpointContentsIter::new(&c, content_bytes)
                    .expect("Checkpoint content iter creation must not fail");

                let _ = summary_iter
                    .zip(content_iter)
                    .filter(|(s, _c)| {
                        s.sequence_number >= checkpoint_range.start
                            && s.sequence_number < checkpoint_range.end
                    })
                    .map(|(summary, contents)| {
                        // Verify summary
                        let prev_checkpoint_seq_num = summary.sequence_number.saturating_sub(1);
                        let prev_checkpoint = store
                            .get_checkpoint_by_sequence_number(prev_checkpoint_seq_num)
                            .map_err(|_| {
                                anyhow!("Failed to read checkpoint: {}", prev_checkpoint_seq_num)
                            })?
                            .context(anyhow!(
                                "Missing previous checkpoint {} in store",
                                prev_checkpoint_seq_num
                            ))?;
                        let verified_checkpoint =
                            verify_checkpoint(&prev_checkpoint, &store, summary).map_err(|_| {
                                anyhow!(
                                    "Checkpoint verification failed for: {}",
                                    prev_checkpoint_seq_num.saturating_add(1)
                                )
                            })?;
                        // Verify content
                        let digest = verified_checkpoint.content_digest;
                        contents.verify_digests(digest)?;
                        let verified_contents =
                            VerifiedCheckpointContents::new_unchecked(contents.clone());
                        // Insert content
                        store
                            .insert_checkpoint_contents(&verified_checkpoint, verified_contents)
                            .map_err(|e| {
                                anyhow!(
                                    "Failed to insert checkpoint {}, err: {:?}",
                                    verified_checkpoint.sequence_number,
                                    e
                                )
                            })?;
                        Ok::<(), anyhow::Error>(())
                    });
            })
            .collect()
            .await;
        results
            .into_iter()
            .collect::<Result<Vec<()>, anyhow::Error>>()?;
        Ok(())
    }

    pub async fn latest_available_checkpoint(&self) -> Result<CheckpointSequenceNumber> {
        let manifest = self.manifest.lock().await.clone();
        Ok(manifest.next_checkpoint_seq_num().saturating_sub(1))
    }

    fn epoch_dir(epoch_num: u64) -> Path {
        Path::from(format!("{}{}", EPOCH_DIR_PREFIX, epoch_num))
    }
}

/// An iterator over all checkpoints in a *.chk file.
pub struct CheckpointContentsIter {
    reader: Box<dyn Read>,
}

impl CheckpointContentsIter {
    pub fn new(file_metadata: &FileMetadata, bytes: Bytes) -> Result<Self> {
        let mut reader = file_metadata.file_compression.stream_decompress(bytes)?;
        let magic = reader.read_u32::<BigEndian>()?;
        if magic != CHECKPOINT_FILE_MAGIC {
            Err(anyhow!(
                "Unexpected magic string in checkpoint file: {:?}",
                magic
            ))
        } else {
            Ok(CheckpointContentsIter { reader })
        }
    }

    fn next_checkpoint(&mut self) -> Result<CheckpointContents> {
        let len = self.reader.read_varint::<u64>()? as usize;
        if len == 0 {
            return Err(anyhow!("Invalid checkpoint length of 0 in file"));
        }
        let encoding = self.reader.read_u8()?;
        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data)?;
        let blob = Blob {
            data,
            encoding: Encoding::try_from(encoding)?,
        };
        blob.decode()
    }
}

impl Iterator for CheckpointContentsIter {
    type Item = CheckpointContents;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_checkpoint().ok()
    }
}

/// An iterator over all checkpoint summaries in a *.chk file.
pub struct CheckpointSummaryIter {
    reader: Box<dyn Read>,
}

impl CheckpointSummaryIter {
    pub fn new(file_metadata: &FileMetadata, bytes: Bytes) -> Result<Self> {
        let mut reader = file_metadata.file_compression.stream_decompress(bytes)?;
        let magic = reader.read_u32::<BigEndian>()?;
        if magic != SUMMARY_FILE_MAGIC {
            Err(anyhow!(
                "Unexpected magic string in checkpoint file: {:?}",
                magic
            ))
        } else {
            Ok(CheckpointSummaryIter { reader })
        }
    }

    fn next_checkpoint(&mut self) -> Result<Checkpoint> {
        let len = self.reader.read_varint::<u64>()? as usize;
        if len == 0 {
            return Err(anyhow!("Invalid checkpoint length of 0 in file"));
        }
        let encoding = self.reader.read_u8()?;
        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data)?;
        let blob = Blob {
            data,
            encoding: Encoding::try_from(encoding)?,
        };
        blob.decode()
    }
}

impl Iterator for CheckpointSummaryIter {
    type Item = Checkpoint;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_checkpoint().ok()
    }
}
