// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]

#[cfg(test)]
mod tests;

mod reader;
mod writer;

use anyhow::Result;
use bytes::{Buf, Bytes};
use fastcrypto::hash::{HashFunction, Sha3_256};
use integer_encoding::VarInt;
use num_enum::IntoPrimitive;
use num_enum::TryFromPrimitive;
use object_store::path::Path;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};
use sui_core::authority::authority_store_tables::AuthorityPerpetualTables;
use sui_core::authority::epoch_start_configuration::EpochStartConfiguration;
use sui_core::checkpoints::CheckpointStore;
use sui_core::epoch::committee_store::CommitteeStore;
use sui_storage::object_store::util::path_to_filesystem;
use sui_types::accumulator::Accumulator;
use sui_types::base_types::ObjectID;
use sui_types::sui_system_state::epoch_start_sui_system_state::EpochStartSystemStateTrait;
use sui_types::sui_system_state::get_sui_system_state;
use sui_types::sui_system_state::SuiSystemStateTrait;

/// The following describes the format of an object file (*.obj) used for persisting live sui objects.
/// The maximum size per .obj file is 128MB. State snapshot will be taken at the end of every epoch.
/// Live object set is split into and stored across multiple hash buckets. The hashing function used
/// for bucketing objects is the same as the one used to build the accumulator tree for computing
/// state root hash. Buckets are further subdivided into partitions. A partition is a smallest storage
/// unit which holds a subset of objects in one bucket. Each partition is a single *.obj file where
/// objects are appended to in an append-only fashion. A new partition is created once the size of
/// current one reaches the max size i.e. 128MB. Partitions allow a single hash bucket to be consumed
/// in parallel. Partition files are optionally compressed with the zstd compression format. Partition
/// filenames follows the format <bucket_number>_<partition_number>.obj. Object references for hash
/// There is one single ref file per hash bucket. Object references are written in an append-only manner
/// as well. Finally, the MANIFEST file contains per file metadata of every file in the snapshot directory.
/// current one reaches the max size i.e. 64MB. Partitions allow a single hash bucket to be consumed
/// in parallel. Partition files are compressed with the zstd compression format which can be read as
/// a decompressed stream (when reading snapshot from a remote cloud object store this doesn't require
///        - 1_1.obj
///        - 1_2.obj
///        - 1_3.obj
///        - 2_1.obj
///        - ...
///        - 1000_1.obj
///        - REFERENCE-1
///        - REFERENCE-2
///        - ...
///        - REFERENCE-1000
///        - MANIFEST
///     - epoch_1/
///       - 1_1.obj
///       - ...
/// Object File Disk Format
///┌──────────────────────────────┐
///│  magic(0x00B7EC75) <4 byte>  │
///├──────────────────────────────┤
///│ ┌──────────────────────────┐ │
///│ │         Object 1         │ │
///│ ├──────────────────────────┤ │
///│ │          ...             │ │
///│ ├──────────────────────────┤ │
///│ │         Object N         │ │
///│ └──────────────────────────┘ │
///└──────────────────────────────┘
/// Object
///┌───────────────┬───────────────────┬──────────────┐
///│ len <uvarint> │ encoding <1 byte> │ data <bytes> │
///└───────────────┴───────────────────┴──────────────┘
///
/// REFERENCE File Disk Format
///┌──────────────────────────────┐
///│  magic(0x5EFE5E11) <4 byte>  │
///├──────────────────────────────┤
///│ ┌──────────────────────────┐ │
///│ │         ObjectRef 1      │ │
///│ ├──────────────────────────┤ │
///│ │          ...             │ │
///│ ├──────────────────────────┤ │
///│ │         ObjectRef N      │ │
///│ └──────────────────────────┘ │
///└──────────────────────────────┘
/// ObjectRef (ObjectID, SequenceNumber, ObjectDigest)
///┌───────────────┬───────────────────┬──────────────┐
///│         data (<(address_len + 8 + 32) bytes>)    │
///└───────────────┴───────────────────┴──────────────┘
///
/// MANIFEST File Disk Format
///┌──────────────────────────────┐
///│  magic(0x00C0FFEE) <4 byte>  │
///├──────────────────────────────┤
///│   serialized manifest        │
///├──────────────────────────────┤
///│      sha3 <32 bytes>         │
///└──────────────────────────────┘
const OBJECT_FILE_MAGIC: u32 = 0x00B7EC75;
const REFERENCE_FILE_MAGIC: u32 = 0xDEADBEEF;
const MANIFEST_FILE_MAGIC: u32 = 0x00C0FFEE;
const MAGIC_BYTES: usize = 4;
const SNAPSHOT_VERSION_BYTES: usize = 1;
const ADDRESS_LENGTH_BYTES: usize = 8;
const PADDING_BYTES: usize = 3;
const MANIFEST_FILE_HEADER_BYTES: usize =
    MAGIC_BYTES + SNAPSHOT_VERSION_BYTES + ADDRESS_LENGTH_BYTES + PADDING_BYTES;
const FILE_MAX_BYTES: usize = 128 * 1024 * 1024 * 1024;
const BLOB_ENCODING_BYTES: usize = 1;
const OBJECT_ID_BYTES: usize = ObjectID::LENGTH;
const SEQUENCE_NUM_BYTES: usize = 8;
const OBJECT_DIGEST_BYTES: usize = 32;
const OBJECT_REF_BYTES: usize = OBJECT_ID_BYTES + SEQUENCE_NUM_BYTES + OBJECT_DIGEST_BYTES;
const MAX_VARINT_LENGTH: usize = 5;
const FILE_TYPE_BYTES: usize = 1;
const BUCKET_BYTES: usize = 4;
const BUCKET_PARTITION_BYTES: usize = 4;
const COMPRESSION_TYPE_BYTES: usize = 1;
const SHA3_BYTES: usize = 32;
const FILE_METADATA_BYTES: usize =
    FILE_TYPE_BYTES + BUCKET_BYTES + BUCKET_PARTITION_BYTES + COMPRESSION_TYPE_BYTES + SHA3_BYTES;

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u8)]
pub enum FileType {
    Object = 0,
    Reference,
}

#[derive(
    Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u8)]
pub enum FileCompression {
    None = 0,
    Zstd,
}

impl FileCompression {
    fn zstd_compress(source: &std::path::Path) -> io::Result<()> {
        let mut file = File::open(source)?;
        let tmp_file_name = source.with_extension("obj.tmp");
        let mut encoder = {
            let target = File::create(&tmp_file_name)?;
            // TODO: Add zstd compression level as function argument
            zstd::Encoder::new(target, 1)?
        };
        io::copy(&mut file, &mut encoder)?;
        encoder.finish()?;
        fs::rename(tmp_file_name, source)?;
        Ok(())
    }
    pub fn compress(&self, source: &std::path::Path) -> io::Result<()> {
        match self {
            FileCompression::Zstd => {
                Self::zstd_compress(source)?;
            }
            FileCompression::None => {}
        }
        Ok(())
    }
    pub fn decompress(&self, source: &PathBuf) -> Result<Box<dyn Read>> {
        let file = File::open(source)?;
        let res: Box<dyn Read> = match self {
            FileCompression::Zstd => Box::new(zstd::stream::Decoder::new(file)?),
            FileCompression::None => Box::new(BufReader::new(file)),
        };
        Ok(res)
    }
    pub fn stream_decompress(&self, bytes: Bytes) -> Result<Box<dyn Read>> {
        let res: Box<dyn Read> = match self {
            FileCompression::Zstd => Box::new(zstd::stream::Decoder::new(bytes.reader())?),
            FileCompression::None => Box::new(BufReader::new(bytes.reader())),
        };
        Ok(res)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct FileMetadata {
    pub file_type: FileType,
    pub bucket_num: u32,
    pub part_num: u32,
    pub file_compression: FileCompression,
    pub sha3_digest: [u8; 32],
}

impl FileMetadata {
    pub fn file_path(&self, dir_path: &Path) -> Path {
        match self.file_type {
            FileType::Object => {
                dir_path.child(&*format!("{}_{}.obj", self.bucket_num, self.part_num))
            }
            FileType::Reference => dir_path.child(&*format!("REFERENCE-{}", self.bucket_num)),
        }
    }
    pub fn local_file_path(&self, root_path: &std::path::Path, dir_path: &Path) -> Result<PathBuf> {
        path_to_filesystem(root_path.to_path_buf(), &self.file_path(dir_path))
    }
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ManifestV1 {
    pub snapshot_version: u8,
    pub address_length: u64,
    pub file_metadata: Vec<FileMetadata>,
    pub epoch: u64,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum Manifest {
    V1(ManifestV1),
}

impl Manifest {
    pub fn snapshot_version(&self) -> u8 {
        match self {
            Self::V1(manifest) => manifest.snapshot_version,
        }
    }
    pub fn address_length(&self) -> u64 {
        match self {
            Self::V1(manifest) => manifest.address_length,
        }
    }
    pub fn file_metadata(&self) -> &Vec<FileMetadata> {
        match self {
            Self::V1(manifest) => &manifest.file_metadata,
        }
    }
    pub fn epoch(&self) -> u64 {
        match self {
            Self::V1(manifest) => manifest.epoch,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum Encoding {
    Bcs = 1,
}

pub struct Blob {
    pub data: Vec<u8>,
    pub encoding: Encoding,
}

impl Blob {
    pub fn encode<T: Serialize>(value: &T, encoding: Encoding) -> Result<Self> {
        let value_buf = bcs::to_bytes(value)?;
        let (data, encoding) = match encoding {
            Encoding::Bcs => (value_buf, encoding),
        };
        Ok(Blob { data, encoding })
    }
    pub fn decode<T: DeserializeOwned>(self) -> Result<T> {
        let data = match &self.encoding {
            Encoding::Bcs => self.data,
        };
        let res = bcs::from_bytes(&data)?;
        Ok(res)
    }
    pub fn append_to_file(&self, wbuf: &mut BufWriter<File>) -> Result<usize> {
        let mut buf = [0u8; MAX_VARINT_LENGTH];
        let mut counter = 0;
        let n = (self.data.len() as u64).encode_var(&mut buf);
        wbuf.write_all(&buf[0..n])?;
        counter += n;
        buf[0] = self.encoding.into();
        wbuf.write_all(&buf[0..BLOB_ENCODING_BYTES])?;
        counter += 1;
        wbuf.write_all(&self.data)?;
        counter += self.data.len();
        Ok(counter)
    }
}

pub fn compute_sha3_checksum_for_file(file: &mut File) -> Result<[u8; 32]> {
    let mut hasher = Sha3_256::default();
    io::copy(file, &mut hasher)?;
    Ok(hasher.finalize().digest)
}

pub fn compute_sha3_checksum(source: &std::path::Path) -> Result<[u8; 32]> {
    let mut file = fs::File::open(source)?;
    compute_sha3_checksum_for_file(&mut file)
}

pub fn create_file_metadata(
    file_path: &std::path::Path,
    file_compression: FileCompression,
    file_type: FileType,
    bucket_num: u32,
    part_num: u32,
) -> Result<FileMetadata> {
    file_compression.compress(file_path)?;
    let sha3_digest = compute_sha3_checksum(file_path)?;
    let file_metadata = FileMetadata {
        file_type,
        bucket_num,
        part_num,
        file_compression,
        sha3_digest,
    };
    Ok(file_metadata)
}

pub async fn setup_db_state(
    epoch: u64,
    accumulator: Accumulator,
    perpetual_db: Arc<AuthorityPerpetualTables>,
    checkpoint_store: Arc<CheckpointStore>,
    committee_store: Arc<CommitteeStore>,
) -> Result<()> {
    // This function should be called once state accumulator based hash verification
    // is complete and live object set state is downloaded to local store
    let system_state_object = get_sui_system_state(&perpetual_db)?;
    let new_epoch_start_state = system_state_object.into_epoch_start_state();
    let next_epoch_committee = new_epoch_start_state.get_sui_committee();
    let last_checkpoint = checkpoint_store
        .get_epoch_last_checkpoint(epoch)
        .expect("Error loading last checkpoint for current epoch")
        .expect("Could not load last checkpoint for current epoch");
    let epoch_start_configuration =
        EpochStartConfiguration::new(new_epoch_start_state, *last_checkpoint.digest());
    perpetual_db
        .set_epoch_start_configuration(&epoch_start_configuration)
        .await?;
    perpetual_db.insert_root_state_hash(epoch, last_checkpoint.sequence_number, accumulator)?;
    perpetual_db.set_highest_pruned_checkpoint_without_wb(last_checkpoint.sequence_number)?;
    committee_store.insert_new_committee(&next_epoch_committee)?;
    checkpoint_store.update_highest_executed_checkpoint(&last_checkpoint)?;

    Ok(())
}