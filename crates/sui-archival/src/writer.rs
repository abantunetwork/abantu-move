// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

use crate::{
    create_file_metadata, read_manifest, CheckpointUpdates, FileCompression, FileMetadata,
    FileType, Manifest, CHECKPOINT_FILE_MAGIC, CHECKPOINT_FILE_SUFFIX, EPOCH_DIR_PREFIX,
    FILE_MAX_BYTES, MAGIC_BYTES, SUMMARY_FILE_MAGIC, SUMMARY_FILE_SUFFIX,
};
use anyhow::Context;
use anyhow::Result;
use byteorder::{BigEndian, ByteOrder};
use integer_encoding::VarInt;
use object_store::DynObjectStore;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use sui_core::authority::authority_store_tables::AuthorityPerpetualTables;
use sui_core::checkpoints::CheckpointStore;
use sui_storage::object_store::util::{copy_file, path_to_filesystem};
use sui_storage::object_store::ObjectStoreConfig;
use sui_storage::{Blob, Encoding, BLOB_ENCODING_BYTES};
use sui_types::base_types::ExecutionData;
use sui_types::messages_checkpoint::{
    CertifiedCheckpointSummary as Checkpoint, FullCheckpointContents as CheckpointContents,
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

/// CheckpointWriterV1 writes checkpoints and summaries. It creates multiple *.chk and *.sum files
struct CheckpointWriterV1 {
    root_dir_path: PathBuf,
    epoch_num: u64,
    checkpoint_range: Range<u64>,
    wbuf: BufWriter<File>,
    summary_wbuf: BufWriter<File>,
    sender: Sender<CheckpointUpdates>,
    checkpoint_buf_offset: usize,
    file_compression: FileCompression,
    manifest: Manifest,
}

impl CheckpointWriterV1 {
    fn new(
        root_dir_path: PathBuf,
        file_compression: FileCompression,
        sender: Sender<CheckpointUpdates>,
        manifest: Manifest,
    ) -> Result<Self> {
        let epoch_num = manifest.epoch_num();
        let checkpoint_sequence_num = manifest.next_checkpoint_seq_num();
        let epoch_dir = root_dir_path.join(format!("{}{epoch_num}", EPOCH_DIR_PREFIX));
        if epoch_dir.exists() {
            fs::remove_dir_all(&epoch_dir)?;
            fs::create_dir_all(&epoch_dir)?;
        }
        let checkpoint_file = Self::next_file(
            &epoch_dir,
            checkpoint_sequence_num,
            CHECKPOINT_FILE_SUFFIX,
            CHECKPOINT_FILE_MAGIC,
        )?;
        let summary_file = Self::next_file(
            &epoch_dir,
            checkpoint_sequence_num,
            SUMMARY_FILE_SUFFIX,
            SUMMARY_FILE_MAGIC,
        )?;
        Ok(CheckpointWriterV1 {
            root_dir_path,
            epoch_num,
            checkpoint_range: checkpoint_sequence_num..checkpoint_sequence_num,
            wbuf: BufWriter::new(checkpoint_file),
            summary_wbuf: BufWriter::new(summary_file),
            checkpoint_buf_offset: 0,
            sender,
            file_compression,
            manifest,
        })
    }
    pub async fn write(
        &mut self,
        checkpoint_contents: CheckpointContents,
        checkpoint_summary: Checkpoint,
    ) -> Result<()> {
        assert_eq!(
            checkpoint_summary.sequence_number,
            self.checkpoint_range.end
        );

        if checkpoint_summary.epoch() == self.epoch_num.saturating_add(1) {
            self.cut().await?;
            self.update_to_next_epoch();
            fs::remove_dir_all(self.epoch_dir())?;
            fs::create_dir_all(self.epoch_dir())?;
            self.reset_checkpoint_range();
            self.create_new_files()?;
        }

        assert_eq!(checkpoint_summary.epoch, self.epoch_num);

        assert_eq!(
            checkpoint_summary.content_digest,
            *checkpoint_contents.checkpoint_contents().digest()
        );

        let contents_blob = Blob::encode(&checkpoint_contents, Encoding::Bcs)?;
        let mut blob_size = contents_blob.data.len().required_space();
        blob_size += BLOB_ENCODING_BYTES;
        blob_size += contents_blob.data.len();
        let cut_new_checkpoint_file = (self.checkpoint_buf_offset + blob_size) > FILE_MAX_BYTES;
        if cut_new_checkpoint_file {
            self.cut().await?;
            self.reset_checkpoint_range();
            self.create_new_files()?;
        }
        self.checkpoint_buf_offset += contents_blob.append_to_file(&mut self.wbuf)?;

        let summary_blob = Blob::encode(&checkpoint_summary, Encoding::Bcs)?;
        summary_blob.append_to_file(&mut self.summary_wbuf)?;

        self.checkpoint_range.end = self.checkpoint_range.end.saturating_add(1);
        Ok(())
    }
    async fn finalize(&mut self) -> Result<FileMetadata> {
        self.wbuf.flush()?;
        self.wbuf.get_ref().sync_data()?;
        let off = self.wbuf.get_ref().stream_position()?;
        self.wbuf.get_ref().set_len(off)?;
        let file_path = self.root_dir_path.join(format!(
            "{}.{CHECKPOINT_FILE_SUFFIX}",
            self.checkpoint_range.start
        ));
        let file_metadata = create_file_metadata(
            &file_path,
            self.file_compression,
            FileType::CheckpointContent,
            self.epoch_num,
            self.checkpoint_range.clone(),
        )?;
        Ok(file_metadata)
    }
    async fn finalize_summary(&mut self) -> Result<FileMetadata> {
        self.summary_wbuf.flush()?;
        self.summary_wbuf.get_ref().sync_data()?;
        let off = self.summary_wbuf.get_ref().stream_position()?;
        self.summary_wbuf.get_ref().set_len(off)?;
        let file_path = self.root_dir_path.join(format!(
            "{}.{SUMMARY_FILE_SUFFIX}",
            self.checkpoint_range.start
        ));
        let file_metadata = create_file_metadata(
            &file_path,
            self.file_compression,
            FileType::CheckpointContent,
            self.epoch_num,
            self.checkpoint_range.clone(),
        )?;
        Ok(file_metadata)
    }
    async fn cut(&mut self) -> Result<()> {
        if !self.checkpoint_range.is_empty() {
            let checkpoint_file_metadata = self.finalize().await?;
            let summary_file_metadata = self.finalize_summary().await?;
            let checkpoint_updates = CheckpointUpdates::new(
                self.epoch_num,
                self.checkpoint_range.end,
                checkpoint_file_metadata,
                summary_file_metadata,
                &mut self.manifest,
            );
            self.sender.send(checkpoint_updates).await?;
        }
        Ok(())
    }
    fn next_file(
        dir_path: &Path,
        checkpoint_sequence_num: u64,
        suffix: &str,
        magic_bytes: u32,
    ) -> Result<File> {
        let next_file_path = dir_path.join(format!("{checkpoint_sequence_num}.{suffix}"));
        let next_file_tmp_path = dir_path.join(format!("{checkpoint_sequence_num}.{suffix}.tmp"));
        let mut f = File::create(next_file_tmp_path.clone())?;
        let mut metab = [0u8; MAGIC_BYTES];
        BigEndian::write_u32(&mut metab, magic_bytes);
        f.rewind()?;
        let n = f.write(&metab)?;
        drop(f);
        fs::rename(next_file_tmp_path, next_file_path.clone())?;
        let mut f = OpenOptions::new().append(true).open(next_file_path)?;
        f.seek(SeekFrom::Start(n as u64))?;
        Ok(f)
    }
    fn create_new_files(&mut self) -> Result<()> {
        let f = Self::next_file(
            &self.epoch_dir(),
            self.checkpoint_range.start,
            CHECKPOINT_FILE_SUFFIX,
            CHECKPOINT_FILE_MAGIC,
        )?;
        self.checkpoint_buf_offset = MAGIC_BYTES;
        self.wbuf = BufWriter::new(f);
        let f = Self::next_file(
            &self.epoch_dir(),
            self.checkpoint_range.start,
            SUMMARY_FILE_SUFFIX,
            SUMMARY_FILE_MAGIC,
        )?;
        self.summary_wbuf = BufWriter::new(f);
        Ok(())
    }
    fn reset_checkpoint_range(&mut self) {
        self.checkpoint_range = self.checkpoint_range.end..self.checkpoint_range.end
    }
    fn epoch_dir(&self) -> PathBuf {
        self.root_dir_path
            .join(format!("{}{}", EPOCH_DIR_PREFIX, self.epoch_num))
    }
    fn update_to_next_epoch(&mut self) {
        self.epoch_num = self.epoch_num.saturating_add(1);
    }
}

/// ArchiveWriterV1 archives history by tailing checkpoints writing them to a local staging dir and
/// simultaneously uploading them to a remote object store
pub struct ArchiveWriterV1 {
    file_compression: FileCompression,
    local_staging_dir_root: PathBuf,
    local_object_store: Arc<DynObjectStore>,
    remote_object_store: Arc<DynObjectStore>,
}

impl ArchiveWriterV1 {
    pub async fn new(
        local_store_config: ObjectStoreConfig,
        remote_store_config: ObjectStoreConfig,
        file_compression: FileCompression,
    ) -> Result<Self> {
        Ok(ArchiveWriterV1 {
            file_compression,
            remote_object_store: remote_store_config.make()?,
            local_object_store: local_store_config.make()?,
            local_staging_dir_root: local_store_config.directory.context("Missing local dir")?,
        })
    }

    pub async fn start(
        &self,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
    ) -> Result<tokio::sync::broadcast::Sender<()>> {
        let (sender, receiver) = mpsc::channel::<CheckpointUpdates>(100);
        let (kill_sender, _) = tokio::sync::broadcast::channel::<()>(1);
        self.start_tailing_checkpoints(
            perpetual_db,
            checkpoint_store,
            sender,
            kill_sender.subscribe(),
        )
        .await?;
        self.start_uploading_checkpoints(receiver, kill_sender.subscribe())
            .await?;
        Ok(kill_sender)
    }

    async fn start_tailing_checkpoints(
        &self,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        sender: Sender<CheckpointUpdates>,
        mut kill: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<()> {
        let remote_archive_is_empty = self
            .remote_object_store
            .list_with_delimiter(None)
            .await?
            .common_prefixes
            .is_empty();
        let manifest = if remote_archive_is_empty {
            // Start from genesis
            Manifest::new(0, 0)
        } else {
            read_manifest(
                self.local_staging_dir_root.clone(),
                self.local_object_store.clone(),
                self.remote_object_store.clone(),
            )
            .await?
        };
        let local_staging_dir_root = self.local_staging_dir_root.clone();
        let file_compression = self.file_compression.clone();
        tokio::task::spawn(async move {
            let mut checkpoint_sequence_number = manifest.next_checkpoint_seq_num();
            let mut writer = CheckpointWriterV1::new(
                local_staging_dir_root,
                file_compression,
                sender,
                manifest,
            )?;
            loop {
                tokio::select! {
                    _ = kill.recv() => break,
                    Ok(_) = Self::write_next_checkpoint(checkpoint_sequence_number, perpetual_db.clone(), checkpoint_store.clone(), &mut writer) => {
                        checkpoint_sequence_number = checkpoint_sequence_number.saturating_add(1);
                    },
                    else => {
                        panic!("Terminating data archive write loop");
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
        Ok(())
    }

    async fn start_uploading_checkpoints(
        &self,
        mut update_receiver: Receiver<CheckpointUpdates>,
        mut kill: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<()> {
        let local_staging_dir_root = self.local_staging_dir_root.clone();
        let local_object_store = self.local_object_store.clone();
        let remote_object_store = self.remote_object_store.clone();
        tokio::task::spawn(async move {
            loop {
                tokio::select! {
                    _ = kill.recv() => break,
                    Some(checkpoint_updates) = update_receiver.recv() => {
                        let summary_file_path = checkpoint_updates.summary_file_path();
                        Self::sync_file_to_remote(local_staging_dir_root.clone(), summary_file_path, local_object_store.clone(), remote_object_store.clone()).await?;

                        let content_file_path = checkpoint_updates.content_file_path();
                        Self::sync_file_to_remote(local_staging_dir_root.clone(), content_file_path, local_object_store.clone(), remote_object_store.clone()).await?;

                        let manifest_file_path = checkpoint_updates.manifest_file_path();
                        Self::sync_file_to_remote(local_staging_dir_root.clone(), manifest_file_path, local_object_store.clone(), remote_object_store.clone()).await?;
                    },
                    else => {
                        panic!("Terminating data archive upload loop");
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        });
        Ok(())
    }

    async fn write_next_checkpoint(
        checkpoint_sequence_number: u64,
        perpetual_db: Arc<AuthorityPerpetualTables>,
        checkpoint_store: Arc<CheckpointStore>,
        checkpoint_writer: &mut CheckpointWriterV1,
    ) -> Result<()> {
        loop {
            if let Some(checkpoint_summary) =
                checkpoint_store.get_checkpoint_by_sequence_number(checkpoint_sequence_number)?
            {
                if let Some(contents) =
                    checkpoint_store.get_checkpoint_contents(&checkpoint_summary.content_digest)?
                {
                    let mut transactions = Vec::with_capacity(contents.size());
                    for digests in contents.iter() {
                        if let (Some(t), Some(e)) = (
                            perpetual_db
                                .get_transaction(&digests.transaction)?
                                .map(|e| e.into_inner()),
                            perpetual_db.get_effects(&digests.transaction)?,
                        ) {
                            transactions.push(ExecutionData::new(t, e));
                        } else {
                            break;
                        }
                    }
                    if transactions.len() == contents.size() {
                        let checkpoint_contents =
                            CheckpointContents::from_contents_and_execution_data(
                                contents,
                                transactions.into_iter(),
                            );
                        checkpoint_writer
                            .write(checkpoint_contents, checkpoint_summary.into_inner())
                            .await?;
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn sync_file_to_remote(
        dir: PathBuf,
        path: object_store::path::Path,
        from: Arc<DynObjectStore>,
        to: Arc<DynObjectStore>,
    ) -> Result<()> {
        copy_file(path.clone(), path.clone(), from, to).await?;
        fs::remove_file(path_to_filesystem(dir, &path)?)?;
        Ok(())
    }
}
