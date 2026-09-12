// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::{Arc, LazyLock as Lazy};

use arrow_schema::{Schema, SchemaRef};
use chrono::{DateTime, Utc};
use config::meta::{
    search::StorageType,
    stream::{FileKey, FileSelection, NativePredicateStrategy},
};
use hashbrown::HashMap;
use object_store::ObjectMeta;
use parking_lot::RwLock;

use super::{ACCOUNT_SEPARATOR, TRACE_ID_SEPARATOR};

#[derive(Clone)]
pub struct ScanSelection {
    /// see [`config::meta::stream::FileKey::selection_exact`]
    pub exact: bool,
    pub selection: Option<FileSelection>,
    pub native_predicate: NativePredicateStrategy,
    pub row_group_size: Option<u32>,
}

/// Whole-object facts from an immutable, hydrated active-file snapshot.
/// They prove neither row order nor selected/output cardinality.
#[derive(Clone, Copy)]
pub struct SnapshotStatistics {
    pub records: usize,
    pub compressed_size: usize,
    pub min_ts: i64,
    pub max_ts: i64,
}

struct RegisteredFile {
    selection: Option<ScanSelection>,
    statistics: Option<SnapshotStatistics>,
}

struct RegisteredFiles {
    owner_trace_id: String,
    objects: Arc<[ObjectMeta]>,
    schema: SchemaRef,
    files: HashMap<String, RegisteredFile>,
}

static FILES: Lazy<RwLock<HashMap<String, RegisteredFiles>>> = Lazy::new(Default::default);

pub fn get(trace_id: &str) -> Result<Arc<[ObjectMeta]>, anyhow::Error> {
    FILES
        .read()
        .get(trace_id)
        .map(|data| Arc::clone(&data.objects))
        .ok_or_else(|| anyhow::anyhow!("trace_id not found: {}", trace_id))
}

/// Resolve an unknown listing size instead of letting DataFusion silently
/// discard a zero-sized placeholder. Normal immutable snapshot listings do
/// not pay for a HEAD. Keep the request/account-qualified location intact.
pub async fn resolve_listing_object(
    mut object: ObjectMeta,
    store: &dyn object_store::ObjectStore,
) -> object_store::Result<Option<ObjectMeta>> {
    if object.size > 0 {
        return Ok(Some(object));
    }
    match store
        .get_opts(
            &object.location,
            object_store::GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
    {
        Ok(result) => {
            object.size = result.meta.size;
            object.last_modified = result.meta.last_modified;
            Ok(Some(object))
        }
        Err(error) => {
            if super::super::vix_format::reconcile_missing_listing_object(
                &object.location,
                &error.to_string(),
            ) {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

fn snapshot_statistics(file: &FileKey, storage_type: &StorageType) -> Option<SnapshotStatistics> {
    // ID-less inputs include WAL and ad-hoc/compaction registrations. Numeric
    // plausibility alone is not a publication proof. Only active persisted DATA
    // identities are eligible; legacy/default and all uncertain facts fall back
    // to the container. Account remains part of the registered object identity.
    if storage_type != &StorageType::Memory
        || file.id <= 0
        || file.deleted
        || !file.key.starts_with("files/")
        || !file.key.ends_with(".vix")
        || file.key.split('/').count() != 9
        || file.key.split('/').any(str::is_empty)
        || file.meta.records <= 0
        || file.meta.compressed_size <= 0
        || file.meta.min_ts <= 0
        || file.meta.max_ts < file.meta.min_ts
        || DateTime::<Utc>::from_timestamp_micros(file.meta.max_ts).is_none()
    {
        return None;
    }
    Some(SnapshotStatistics {
        records: usize::try_from(file.meta.records).ok()?,
        compressed_size: usize::try_from(file.meta.compressed_size).ok()?,
        min_ts: file.meta.min_ts,
        max_ts: file.meta.max_ts,
    })
}

pub async fn set(
    trace_id: &str,
    owner_trace_id: &str,
    schema_key: &str,
    format: &str,
    files: Vec<FileKey>,
    storage_type: StorageType,
    schema: SchemaRef,
) {
    let key = format!("{trace_id}/schema={schema_key}/format={format}");
    let mut objects = Vec::with_capacity(files.len());
    let mut registered = HashMap::with_capacity(files.len());
    for file in files {
        // Bad historical ranges must not overflow while constructing a listing;
        // their metadata is untrusted and the footer supplies the real facts.
        let modified = DateTime::<Utc>::from_timestamp_micros(file.meta.max_ts).unwrap_or_default();
        let filename = if file.account.is_empty() {
            file.key.clone()
        } else {
            format!("{}/{}/{}", file.account, ACCOUNT_SEPARATOR, file.key)
        };
        let statistics = snapshot_statistics(&file, &storage_type);
        objects.push(ObjectMeta {
            location: format!("/{key}/{TRACE_ID_SEPARATOR}/{filename}").into(),
            last_modified: modified,
            size: u64::try_from(file.meta.compressed_size).unwrap_or_default(),
            e_tag: None,
            version: None,
        });
        registered.insert(
            filename,
            RegisteredFile {
                statistics,
                selection: (file.selection.is_some()
                    || file.native_predicate != NativePredicateStrategy::default())
                .then_some(ScanSelection {
                    exact: file.selection_exact,
                    selection: file.selection,
                    native_predicate: file.native_predicate,
                    row_group_size: file.row_group_size,
                }),
            },
        );
    }
    FILES.write().insert(
        key,
        RegisteredFiles {
            owner_trace_id: owner_trace_id.to_string(),
            objects: objects.into(),
            schema,
            files: registered,
        },
    );
}

pub fn clear(trace_id: &str) {
    // Listing session ids may differ across storage/WAL and time/format
    // partitions. Their explicit request owner is the cleanup authority.
    FILES
        .write()
        .retain(|_, registration| registration.owner_trace_id != trace_id);
}

pub fn get_scan_selection(file_key: &str) -> Option<ScanSelection> {
    let (trace_id, filename) = file_key.split_once("/$$/")?;
    FILES
        .read()
        .get(trace_id)?
        .files
        .get(filename)?
        .selection
        .clone()
}

pub fn get_snapshot_statistics(object: &ObjectMeta, schema: &Schema) -> Option<SnapshotStatistics> {
    let (trace_id, filename) = object.location.as_ref().split_once("/$$/")?;
    let registrations = FILES.read();
    let registration = registrations.get(trace_id)?;
    if registration.schema.as_ref() != schema {
        return None;
    }
    let statistics = registration.files.get(filename)?.statistics?;
    (u64::try_from(statistics.compressed_size).ok()? == object.size).then_some(statistics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn registry_keeps_accounts_and_request_lifetimes_separate() {
        use config::meta::stream::{FileMeta, RowIdBitmap};
        let schema = Arc::new(Schema::empty());
        let key = "files/default/logs/registry/2026/01/01/00/a.vix";
        let mut first = FileKey::new(
            1,
            "a".into(),
            key.into(),
            FileMeta {
                records: 2,
                compressed_size: 100,
                min_ts: 10,
                max_ts: 20,
                ..Default::default()
            },
            false,
        );
        first.with_selection(
            FileSelection::Rows(Arc::new(RowIdBitmap::from_row_ids(2, [0]))),
            None,
        );
        first.selection_exact = true;
        let mut second = first.clone();
        second.account = "b".into();
        second.meta.records = 3;
        second.selection = None;
        set(
            "registry-identity",
            "registry-identity",
            "schema",
            "vix",
            vec![first, second],
            StorageType::Memory,
            schema.clone(),
        )
        .await;
        set(
            "registry-identity-other",
            "registry-identity-other",
            "schema",
            "vix",
            vec![],
            StorageType::Wal,
            schema.clone(),
        )
        .await;
        let objects = get("registry-identity/schema=schema/format=vix").unwrap();
        assert_eq!(
            get_snapshot_statistics(&objects[0], &schema)
                .unwrap()
                .records,
            2
        );
        assert_eq!(
            get_snapshot_statistics(&objects[1], &schema)
                .unwrap()
                .records,
            3
        );
        assert!(
            get_scan_selection(objects[0].location.as_ref())
                .unwrap()
                .exact
        );
        assert!(get_scan_selection(objects[1].location.as_ref()).is_none());
        let mut changed = objects[0].clone();
        changed.size += 1;
        assert!(get_snapshot_statistics(&changed, &schema).is_none());
        clear("registry-identity");
        assert!(get_scan_selection(objects[0].location.as_ref()).is_none());
        assert!(get("registry-identity-other/schema=schema/format=vix").is_ok());
        clear("registry-identity-other");
    }

    #[tokio::test]
    async fn root_clear_releases_derived_sessions_and_keeps_neighbor_owners() {
        use config::meta::stream::{FileMeta, RowIdBitmap};

        let root = "registry-owner-root";
        let schema = Arc::new(Schema::empty());
        let schema_owner = Arc::downgrade(&schema);
        let mut file = FileKey::new(
            1,
            "account-a".into(),
            "files/default/logs/registry/2026/01/01/00/owned.vix".into(),
            FileMeta {
                records: 2,
                compressed_size: 100,
                min_ts: 10,
                max_ts: 20,
                ..Default::default()
            },
            false,
        );
        let rows = Arc::new(RowIdBitmap::from_row_ids(2, [0]));
        let rows_owner = Arc::downgrade(&rows);
        file.with_selection(FileSelection::Rows(rows), None);
        file.selection_exact = true;
        let mut other_account = file.clone();
        other_account.account = "account-b".into();
        other_account.selection = None;
        let mut listings = Vec::new();
        for suffix in ["storage-true", "storage-false", "wal-true", "wal-false"] {
            let session = format!("{root}-{suffix}");
            set(
                &session,
                root,
                "schema",
                "vix",
                vec![file.clone(), other_account.clone()],
                StorageType::Memory,
                schema.clone(),
            )
            .await;
            listings.push((
                format!("{session}/schema=schema/format=vix"),
                get(&format!("{session}/schema=schema/format=vix")).unwrap(),
            ));
        }
        // A distinct owner may itself look exactly like a derived session.
        // Neither a plain shared prefix nor a suffix heuristic can identify it.
        let neighbor = format!("{root}-storage-true");
        set(
            &neighbor,
            &neighbor,
            "neighbor-schema",
            "vix",
            vec![],
            StorageType::Memory,
            Arc::new(Schema::empty()),
        )
        .await;
        drop(file);
        drop(other_account);
        drop(schema);
        for (_, objects) in &listings {
            assert!(
                get_scan_selection(objects[0].location.as_ref())
                    .unwrap()
                    .exact
            );
            assert!(get_scan_selection(objects[1].location.as_ref()).is_none());
        }

        // Cancellation clears the request while a listing consumer still
        // holds its metadata snapshot; schema and bitmap owners must release.
        clear(root);
        for (key, objects) in &listings {
            assert!(get(key).is_err());
            assert!(get_scan_selection(objects[0].location.as_ref()).is_none());
            assert!(get_snapshot_statistics(&objects[0], &Schema::empty()).is_none());
        }
        assert!(schema_owner.upgrade().is_none());
        assert!(rows_owner.upgrade().is_none());
        assert!(get(&format!("{neighbor}/schema=neighbor-schema/format=vix")).is_ok());
        clear(root); // repeated cancellation/completion cleanup is harmless
        assert!(get(&format!("{neighbor}/schema=neighbor-schema/format=vix")).is_ok());
        clear(&neighbor);
    }
}
