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

use config::meta::stream::{FileKey, FileListDeleted, FileMeta};
use futures::{StreamExt, stream};
use infra::{file_list as infra_file_list, storage};

// Batch size for deleting files from file_list_deleted table
const BATCH_SIZE: i64 = 10000;

pub async fn delete(org_id: &str, time_max: i64) -> Result<i64, anyhow::Error> {
    let files = infra_file_list::query_deleted(org_id, time_max, BATCH_SIZE).await?;
    if files.is_empty() {
        return Ok(0);
    }
    let concurrency = config::get_config().limit.cpu_num.max(1);
    let deleted = stream::iter(files.into_iter().map(delete_one))
        .buffer_unordered(concurrency)
        .filter_map(std::future::ready)
        .collect::<Vec<_>>()
        .await;

    if deleted.is_empty() {
        return Ok(0);
    }
    if let Err(e) = infra_file_list::batch_remove_deleted(&deleted).await {
        log::error!("[COMPACTOR] delete files from table failed: {e}");
        return Err(e.into());
    }

    Ok(deleted.len() as i64)
}

async fn delete_one(file: FileListDeleted) -> Option<FileKey> {
    let storage_deleted = if ingester::is_wal_file(&file.file) {
        true
    } else {
        match storage::delete(&file.account, &file.file).await {
            Ok(()) => {
                if let Some(sidecar) = config::vix_sidecar_key(&file.file, file.index_generation) {
                    match storage::delete(&file.account, &sidecar).await {
                        Ok(()) => true,
                        Err(e) => {
                            log::error!(
                                "[COMPACTOR] delete sidecar {sidecar} failed; retaining \
                                 deletion row for retry: {e}"
                            );
                            false
                        }
                    }
                } else {
                    true
                }
            }
            Err(e) => {
                log::error!(
                    "[COMPACTOR] delete object {} failed; retaining deletion row for retry: {e}",
                    file.file
                );
                false
            }
        }
    };
    storage_deleted.then(|| completed_row(file))
}

fn completed_row(file: FileListDeleted) -> FileKey {
    FileKey::new(file.id, file.account, file.file, FileMeta::default(), false)
}
