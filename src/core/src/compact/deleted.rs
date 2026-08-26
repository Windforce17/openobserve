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

use config::meta::stream::{FileKey, FileMeta};
use infra::{file_list as infra_file_list, storage};

// Batch size for deleting files from file_list_deleted table
const BATCH_SIZE: i64 = 10000;

pub async fn delete(org_id: &str, time_max: i64) -> Result<i64, anyhow::Error> {
    let files = infra_file_list::query_deleted(org_id, time_max, BATCH_SIZE).await?;
    if files.is_empty() {
        return Ok(0);
    }
    let files_num = files.len() as i64;

    // delete files from storage. v3 sidecar split: every `.vix` data key
    // gets its `.vxi` sidecar key deleted UNCONDITIONALLY alongside it —
    // simplest correct option over consulting the vestigial
    // `file_list_deleted.index_file` flag (which stays always-false):
    // index-off files simply have no sidecar and the delete tolerates
    // NotFound (`storage::del` already swallows per-key not-found).
    let mut sidecar_keys: Vec<(String, String)> = Vec::new();
    for file in &files {
        if !ingester::is_wal_file(&file.file)
            && let Some(sidecar) = config::vix_sidecar_key(&file.file)
        {
            sidecar_keys.push((file.account.clone(), sidecar));
        }
    }
    let mut del_keys: Vec<(&str, &str)> = files
        .iter()
        .filter_map(|file| {
            if !ingester::is_wal_file(&file.file) {
                Some((file.account.as_str(), file.file.as_str()))
            } else {
                None
            }
        })
        .collect();
    del_keys.extend(
        sidecar_keys
            .iter()
            .map(|(account, key)| (account.as_str(), key.as_str())),
    );
    if let Err(e) = storage::del(del_keys).await {
        // maybe the file already deleted, so we just skip the `not found` error
        if !e.to_string().to_lowercase().contains("not found") {
            log::error!("[COMPACTOR] delete files from storage failed: {e}");
            return Err(e.into());
        }
    }

    // delete files from file_list_deleted table
    if let Err(e) = infra_file_list::batch_remove_deleted(
        &files
            .iter()
            .map(|file| {
                FileKey::new(
                    file.id,
                    file.account.clone(),
                    file.file.clone(),
                    FileMeta::default(),
                    false,
                )
            })
            .collect::<Vec<_>>(),
    )
    .await
    {
        log::error!("[COMPACTOR] delete files from table failed: {e}");
        return Err(e.into());
    }

    Ok(files_num)
}
