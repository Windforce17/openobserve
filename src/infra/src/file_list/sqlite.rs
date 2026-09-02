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

use std::collections::HashMap as stdHashMap;

use async_trait::async_trait;
use config::{
    get_config,
    meta::stream::{
        FileKey, FileListDeleted, FileMeta, PartitionTimeLevel, StreamStats, StreamType,
    },
    utils::{
        parquet::parse_file_key_columns,
        time::{DAY_MICRO_SECS, end_of_the_day, now_micros},
    },
};
use hashbrown::HashMap;
use sqlx::{Executor, QueryBuilder, Row, Sqlite};

use crate::{
    db::{
        IndexStatement,
        sqlite::{CLIENT_RO, CLIENT_RW, add_column, create_index, delete_index, drop_column},
    },
    errors::{Error, Result},
    file_list::FileRecord,
};

pub struct SqliteFileList {}

impl SqliteFileList {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SqliteFileList {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl super::FileList for SqliteFileList {
    async fn health_check(&self) -> Result<()> {
        Ok(())
    }

    async fn create_table(&self) -> Result<()> {
        create_table().await
    }

    async fn create_table_index(&self) -> Result<()> {
        create_table_index().await
    }

    async fn add(&self, account: &str, file: &str, meta: &FileMeta) -> Result<i64> {
        self.inner_add("file_list", account, file, meta).await
    }

    async fn add_history(&self, account: &str, file: &str, meta: &FileMeta) -> Result<i64> {
        self.inner_add("file_list_history", account, file, meta)
            .await
    }

    async fn remove(&self, file: &str) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let pool = client.clone();
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).map_err(|e| Error::Message(e.to_string()))?;

        sqlx::query(r#"DELETE FROM file_list WHERE stream = $1 AND date = $2 AND file = $3;"#)
            .bind(stream_key)
            .bind(date_key)
            .bind(file_name)
            .execute(&pool)
            .await?;
        Ok(())
    }

    async fn batch_add(&self, files: &[FileKey]) -> Result<()> {
        self.inner_batch_process("file_list", files).await
    }

    async fn batch_add_with_id(&self, files: &[FileKey]) -> Result<()> {
        self.inner_batch_process("file_list", files).await
    }

    async fn batch_add_history(&self, files: &[FileKey]) -> Result<()> {
        self.inner_batch_process("file_list_history", files).await
    }

    async fn batch_process(&self, files: &[FileKey]) -> Result<()> {
        self.inner_batch_process("file_list", files).await
    }

    async fn update_dump_records(&self, file: &FileKey, dumped_ids: &[i64]) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await?;

        // insert the dump file into file_list table
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(&file.key).map_err(|e| Error::Message(e.to_string()))?;
        let org_id = stream_key[..stream_key.find('/').unwrap()].to_string();
        let meta = &file.meta;
        let now_ts = now_micros();

        if let Err(e) = sqlx::query(r#"INSERT INTO file_list (account, org, stream, date, file, deleted, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);"#)
        .bind(&file.account)
        .bind(org_id)
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .bind(false)
        .bind(meta.min_ts)
        .bind(meta.max_ts)
        .bind(meta.records)
        .bind(meta.original_size)
        .bind(meta.compressed_size)
        .bind(meta.index_size)
        .bind(meta.bloom_ver)
        .bind(meta.flattened)
        .bind(now_ts)
        .execute(&mut *tx)
        .await{
            if let Err(e) = tx.rollback().await {
                log::error!("[SQLITE] rollback file_list dump file update error: {e}");
            }
            return Err(e.into());
        }

        // delete the dumped ids from file_list table
        for chunk in dumped_ids.chunks(get_config().compact.file_list_deleted_batch_size) {
            if chunk.is_empty() {
                continue;
            }
            let ids = chunk
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .join(",");
            let query_str = format!("DELETE FROM file_list WHERE id IN ({ids})");
            if let Err(e) = sqlx::query(&query_str).execute(&mut *tx).await {
                if let Err(e) = tx.rollback().await {
                    log::error!("[SQLITE] rollback file_list dump file update error: {e}");
                }
                return Err(e.into());
            }
        }

        if let Err(e) = tx.commit().await {
            log::error!("[SQLITE] transaction commit error for dump file update {e}");
            return Err(e.into());
        }
        Ok(())
    }

    async fn batch_add_deleted(
        &self,
        org_id: &str,
        created_at: i64,
        files: &[FileListDeleted],
    ) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let chunks = files.chunks(100);
        for files in chunks {
            // we don't care the id here, because the id is from file_list table not for this table
            let client = CLIENT_RW.clone();
            let client = client.lock().await;
            let mut tx = client.begin().await?;
            let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
                "INSERT INTO file_list_deleted (account, org, stream, date, file, index_file, flattened, created_at)",
            );
            query_builder.push_values(files, |mut b, item| {
                let (stream_key, date_key, file_name) =
                    parse_file_key_columns(&item.file).expect("parse file key failed");
                b.push_bind(&item.account)
                    .push_bind(org_id)
                    .push_bind(stream_key)
                    .push_bind(date_key)
                    .push_bind(file_name)
                    .push_bind(item.index_file)
                    .push_bind(item.flattened)
                    .push_bind(created_at);
            });
            if let Err(e) = query_builder.build().execute(&mut *tx).await {
                if let Err(e) = tx.rollback().await {
                    log::error!("[SQLITE] rollback file_list_deleted batch add error: {e}");
                }
                return Err(e.into());
            };
            if let Err(e) = tx.commit().await {
                log::error!("[SQLITE] commit file_list_deleted batch add error: {e}");
                return Err(e.into());
            }
        }
        Ok(())
    }

    async fn batch_remove_deleted(&self, files: &[FileKey]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let chunks = files.chunks(100);
        for files in chunks {
            // get ids of the files
            let client = CLIENT_RW.clone();
            let client = client.lock().await;
            let pool = client.clone();
            let mut ids = Vec::with_capacity(files.len());
            for file in files {
                if file.id > 0 {
                    ids.push(file.id.to_string());
                    continue;
                }
                let (stream_key, date_key, file_name) =
                    parse_file_key_columns(&file.key).map_err(|e| Error::Message(e.to_string()))?;
                let ret: Option<i64> = match sqlx::query_scalar(
                    r#"SELECT id FROM file_list_deleted WHERE stream = $1 AND date = $2 AND file = $3;"#,
                )
                .bind(stream_key)
                .bind(date_key)
                .bind(file_name)
                .fetch_one(&pool)
                .await
                {
                    Ok(v) => v,
                    Err(sqlx::Error::RowNotFound) => continue,
                    Err(e) => return Err(e.into()),
                };
                match ret {
                    Some(v) => ids.push(v.to_string()),
                    None => {
                        return Err(Error::Message(
                            "[SQLITE] query error: id should not empty from file_list_deleted"
                                .to_string(),
                        ));
                    }
                }
            }
            // delete files by ids
            if !ids.is_empty() {
                let sql = format!(
                    "DELETE FROM file_list_deleted WHERE id IN({});",
                    ids.join(",")
                );
                _ = pool.execute(sql.as_str()).await?;
            }
        }
        Ok(())
    }

    async fn get(&self, file: &str) -> Result<FileMeta> {
        let pool = CLIENT_RO.clone();
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).map_err(|e| Error::Message(e.to_string()))?;
        let ret = sqlx::query_as::<_, super::FileRecord>(
            r#"
SELECT min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened, file, date
    FROM file_list WHERE stream = $1 AND date = $2 AND file = $3;
            "#,
        )
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .fetch_one(&pool)
        .await?;
        Ok(FileMeta::from(&ret))
    }

    async fn contains(&self, file: &str) -> Result<bool> {
        let pool = CLIENT_RO.clone();
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).map_err(|e| Error::Message(e.to_string()))?;
        let ret = sqlx::query(
            r#"SELECT * FROM file_list WHERE stream = $1 AND date = $2 AND file = $3;"#,
        )
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .fetch_one(&pool)
        .await;
        if let Err(sqlx::Error::RowNotFound) = ret {
            return Ok(false);
        }
        Ok(!ret.unwrap().is_empty())
    }

    async fn update_compressed_size(&self, file: &str, size: i64) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).map_err(|e| Error::Message(e.to_string()))?;
        sqlx::query(
            r#"UPDATE file_list SET compressed_size = $1 WHERE stream = $2 AND date = $3 AND file = $4;"#,
        )
        .bind(size)
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .execute(&*client)
        .await?;
        Ok(())
    }

    async fn update_index_size_for_heal(&self, file: &str, index_size: i64) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).map_err(|e| Error::Message(e.to_string()))?;
        sqlx::query(
            r#"UPDATE file_list SET index_size = $1, bloom_ver = 0 WHERE stream = $2 AND date = $3 AND file = $4;"#,
        )
        .bind(index_size)
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .execute(&*client)
        .await?;
        Ok(())
    }

    async fn update_bloom_ver(&self, ids: &[i64], bloom_ver: i64) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        // Chunk in case of very long lists; SQLite caps placeholders at 999
        // and we'd rather not blow it up unexpectedly.
        for chunk in ids.chunks(900) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!("UPDATE file_list SET bloom_ver = ? WHERE id IN ({placeholders});");
            let mut q = sqlx::query(&sql).bind(bloom_ver);
            for id in chunk {
                q = q.bind(*id);
            }
            q.execute(&*client).await?;
        }
        Ok(())
    }

    async fn bloom_ver_referenced(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        date: &str,
        bloom_ver: i64,
    ) -> Result<bool> {
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
        let pool = CLIENT_RO.clone();
        let found: Option<(i64,)> = sqlx::query_as(
            r#"SELECT 1 FROM file_list WHERE stream = $1 AND date = $2 AND bloom_ver = $3 LIMIT 1;"#,
        )
        .bind(stream_key)
        .bind(date)
        .bind(bloom_ver)
        .fetch_optional(&pool)
        .await?;
        Ok(found.is_some())
    }

    async fn list(&self) -> Result<Vec<FileKey>> {
        let pool = CLIENT_RO.clone();
        let ret = sqlx::query_as::<_, super::FileRecord>(
            r#"SELECT id, account, stream, date, file, deleted, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened FROM file_list;"#,
        )
        .fetch_all(&pool)
        .await?;
        Ok(ret.iter().map(|r| r.into()).collect())
    }

    async fn query(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        _time_level: PartitionTimeLevel,
        time_range: (i64, i64),
    ) -> Result<Vec<FileKey>> {
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let pool = CLIENT_RO.clone();
        let (time_start, time_end) = time_range;
        let max_ts_upper_bound = super::calculate_max_ts_upper_bound(time_end, stream_type);
        let ret = sqlx::query_as::<_, super::FileRecord>(
            r#"
SELECT id, account, stream, date, file, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened
    FROM file_list
    WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts <= $4;
                "#,
        )
        .bind(stream_key)
        .bind(time_start)
        .bind(max_ts_upper_bound)
        .bind(time_end)
        .fetch_all(&pool)
        .await;
        Ok(ret?.iter().map(|r| r.into()).collect())
    }

    async fn query_for_merge(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        date_range: (String, String),
        include_oversize: bool,
    ) -> Result<Vec<FileKey>> {
        let (date_start, date_end) = date_range;
        if date_start.is_empty() && date_end.is_empty() {
            return Ok(Vec::new());
        }
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let cfg = get_config();
        let (max_size, indexed_max_size) = if include_oversize {
            (i64::MAX, i64::MAX)
        } else {
            (
                cfg.compact.max_file_size as i64 * 95 / 100,
                cfg.compact.max_file_size_for_merge(stream_type, true) as i64 * 95 / 100,
            )
        };
        let pool = CLIENT_RO.clone();
        let ret = sqlx::query_as::<_, super::FileRecord>(
                r#"
SELECT id, account, stream, date, file, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened
    FROM file_list
    WHERE stream = $1 AND date >= $2 AND date <= $3
        AND (original_size <= $4
            OR (index_size > 0 AND file LIKE '%.vix' AND original_size <= $5));
                "#,
            )
            .bind(stream_key)
            .bind(date_start)
            .bind(date_end)
            .bind(max_size)
            .bind(indexed_max_size)
            .fetch_all(&pool)
            .await;
        Ok(ret?.iter().map(|r| r.into()).collect())
    }

    async fn query_for_dump(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        time_range: (i64, i64),
    ) -> Result<Vec<FileRecord>> {
        let (time_start, time_end) = time_range;
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let pool = CLIENT_RO.clone();
        let max_ts_upper_bound = super::calculate_max_ts_upper_bound(time_end, stream_type);
        let ret = sqlx::query_as::<_, super::FileRecord>(
            r#"SELECT * FROM file_list WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts <= $4;"#,
        )
        .bind(stream_key)
        .bind(time_start)
        .bind(max_ts_upper_bound)
        .bind(time_end)
        .fetch_all(&pool)
        .await;

        Ok(ret?)
    }

    async fn query_for_dump_by_updated_at(
        &self,
        time_range: (i64, i64),
    ) -> Result<Vec<FileRecord>> {
        let (time_start, time_end) = time_range;

        let pool = CLIENT_RO.clone();
        let ret = sqlx::query_as::<_, super::FileRecord>(
            r#"SELECT * FROM file_list WHERE updated_at > $1 AND updated_at <= $2 AND stream LIKE $3;"#,
        )
        .bind(time_start)
        .bind(time_end)
        .bind(format!("%/{}/%", StreamType::Filelist))
        .fetch_all(&pool)
        .await;

        Ok(ret?)
    }

    async fn query_for_bloom(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        date: &str,
    ) -> Result<Vec<FileKey>> {
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let pool = CLIENT_RO.clone();
        let sql = r#"
SELECT id, account, stream, date, file, records, index_size, compressed_size FROM file_list WHERE stream = $1 AND date = $2 AND index_size > 0 AND bloom_ver = 0;
                "#;
        let ret = sqlx::query_as::<_, super::FileRecord>(sql)
            .bind(stream_key)
            .bind(date)
            .fetch_all(&pool)
            .await;
        Ok(ret?.iter().map(|r| r.into()).collect())
    }

    async fn query_bloom_pending_buckets(
        &self,
        before_date: &str,
        limit: i64,
    ) -> Result<Vec<(String, String)>> {
        let pool = CLIENT_RO.clone();
        let sql = r#"
SELECT stream, date FROM file_list WHERE index_size > 0 AND bloom_ver = 0 AND date < $1 GROUP BY stream, date ORDER BY date DESC LIMIT $2;
                "#;
        let ret: Vec<(String, String)> = sqlx::query_as::<_, (String, String)>(sql)
            .bind(before_date)
            .bind(limit)
            .fetch_all(&pool)
            .await?;
        Ok(ret)
    }

    async fn query_by_ids(
        &self,
        ids: &[i64],
        _time_range: Option<(i64, i64)>,
    ) -> Result<Vec<FileKey>> {
        // SQLite backend is not partitioned, the id lookup is already a single
        // index probe; the time range filter is only useful for partition pruning.
        if ids.is_empty() {
            return Ok(Vec::default());
        }

        let mut ret = Vec::new();
        let pool = CLIENT_RO.clone();

        for chunk in ids.chunks(get_config().compact.file_list_deleted_batch_size) {
            if chunk.is_empty() {
                continue;
            }
            let ids = chunk
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<String>>()
                .join(",");
            let query_str = format!(
                "SELECT id, account, stream, date, file, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver FROM file_list WHERE id IN ({ids})"
            );
            let res = sqlx::query_as::<_, super::FileRecord>(&query_str)
                .fetch_all(&pool)
                .await?;
            ret.extend_from_slice(&res);
        }

        Ok(ret.iter().map(|r| r.into()).collect())
    }

    async fn query_ids(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        time_range: (i64, i64),
    ) -> Result<Vec<super::FileId>> {
        let (time_start, time_end) = time_range;
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let day_partitions = if time_end - time_start <= DAY_MICRO_SECS
            || time_end - time_start > DAY_MICRO_SECS * 30
            || !get_config().compact.file_list_multi_thread
        {
            vec![(time_start, time_end)]
        } else {
            let mut partitions = Vec::new();
            let mut start = time_start;
            while start < time_end {
                let end_of_day = std::cmp::min(end_of_the_day(start), time_end);
                partitions.push((start, end_of_day));
                start = end_of_day + 1; // next day, use end_of_day + 1 microsecond
            }
            partitions
        };
        log::debug!("file_list day_partitions: {day_partitions:?}");

        let mut tasks = Vec::with_capacity(day_partitions.len());

        for (time_start, time_end) in day_partitions {
            let stream_key = stream_key.clone();
            tasks.push(tokio::task::spawn(async move {
                let pool = CLIENT_RO.clone();
                    let max_ts_upper_bound = super::calculate_max_ts_upper_bound(time_end, stream_type);
                    let query = "SELECT id, records, original_size FROM file_list WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts < $4;";
                    sqlx::query_as::<_, super::FileId>(query)
                    .bind(stream_key)
                    .bind(time_start)
                    .bind(max_ts_upper_bound)
                    .bind(time_end)
                    .fetch_all(&pool)
                    .await
            }));
        }

        let mut rets = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(r)) => rets.extend(r),
                Ok(Err(e)) => {
                    return Err(e.into());
                }
                Err(e) => {
                    return Err(Error::Message(e.to_string()));
                }
            };
        }
        Ok(rets)
    }

    async fn query_ids_with_file(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        time_range: (i64, i64),
    ) -> Result<Vec<super::FileIdWithFile>> {
        let (time_start, time_end) = time_range;
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let day_partitions = if time_end - time_start <= DAY_MICRO_SECS
            || time_end - time_start > DAY_MICRO_SECS * 30
            || !get_config().compact.file_list_multi_thread
        {
            vec![(time_start, time_end)]
        } else {
            let mut partitions = Vec::new();
            let mut start = time_start;
            while start < time_end {
                let end_of_day = std::cmp::min(end_of_the_day(start), time_end);
                partitions.push((start, end_of_day));
                start = end_of_day + 1;
            }
            partitions
        };
        log::debug!("file_list day_partitions: {day_partitions:?}");

        let mut tasks = Vec::with_capacity(day_partitions.len());

        for (time_start, time_end) in day_partitions {
            let stream_key = stream_key.clone();
            tasks.push(tokio::task::spawn(async move {
                let pool = CLIENT_RO.clone();
                let max_ts_upper_bound =
                    super::calculate_max_ts_upper_bound(time_end, stream_type);
                let query = "SELECT id, file, records, original_size FROM file_list WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts < $4;";
                sqlx::query_as::<_, super::FileIdWithFile>(query)
                    .bind(stream_key)
                    .bind(time_start)
                    .bind(max_ts_upper_bound)
                    .bind(time_end)
                    .fetch_all(&pool)
                    .await
            }));
        }

        let mut rets = Vec::new();
        for task in tasks {
            match task.await {
                Ok(Ok(r)) => rets.extend(r),
                Ok(Err(e)) => {
                    return Err(e.into());
                }
                Err(e) => {
                    return Err(Error::Message(e.to_string()));
                }
            };
        }
        Ok(rets)
    }

    async fn query_ids_by_files(&self, files: &[FileKey]) -> Result<stdHashMap<String, i64>> {
        let mut ret = stdHashMap::with_capacity(files.len());
        // group by date
        let mut stream_files = HashMap::new();
        let mut files_map = HashMap::with_capacity(files.len());
        for file in files {
            if file.id > 0 {
                ret.insert(file.key.clone(), file.id);
                continue;
            }
            let (stream_key, date_key, file_name) =
                parse_file_key_columns(&file.key).map_err(|e| Error::Message(e.to_string()))?;
            let stream_entry = stream_files.entry(stream_key).or_insert(HashMap::new());
            let date_entry = stream_entry.entry(date_key).or_insert(Vec::new());
            date_entry.push(file_name.clone());
            files_map.insert(file_name, &file.key);
        }
        for (stream_key, stream_files) in stream_files {
            let pool = CLIENT_RO.clone();
            for (date_key, files) in stream_files {
                if files.is_empty() {
                    continue;
                }
                let sql = format!(
                    "SELECT id, file FROM file_list WHERE stream = $1 AND date = $2 AND file IN ('{}');",
                    files.join("','")
                );
                let query_res = sqlx::query_as::<_, super::FileIdWithFile>(&sql)
                    .bind(&stream_key)
                    .bind(&date_key)
                    .fetch_all(&pool)
                    .await?;
                for file in query_res {
                    if let Some(file_name) = files_map.get(&file.file) {
                        ret.insert(file_name.to_string(), file.id);
                    }
                }
            }
        }
        Ok(ret)
    }

    async fn query_old_data_hours(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        time_range: (i64, i64),
        include_lone_unindexed: bool,
    ) -> Result<Vec<String>> {
        let (time_start, time_end) = time_range;
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");

        let pool = CLIENT_RO.clone();
        let cfg = get_config();
        let max_ts_upper_bound = super::calculate_max_ts_upper_bound(time_end, stream_type);
        // Indexed .vix rows use the higher dictionary-passthrough target's
        // half-size debt line ($8); flat and index-less rows stay on the
        // rebuild-safe global line ($5). $7 preserves the lone-unindexed
        // healing wedge.
        let sql = r#"
SELECT date
    FROM file_list
    WHERE stream = $1 AND max_ts >= $2 AND max_ts <= $3 AND min_ts <= $4
        AND (original_size <= $5
            OR (index_size > 0 AND file LIKE '%.vix' AND original_size <= $8))
    GROUP BY date
    HAVING count(*) >= $6
        OR ($7 AND sum(CASE WHEN index_size = 0 AND file LIKE '%.vix' THEN 1 ELSE 0 END) > 0);
            "#;

        let ret = sqlx::query(sql)
            .bind(stream_key)
            .bind(time_start)
            .bind(max_ts_upper_bound)
            .bind(time_end)
            .bind(cfg.compact.max_file_size as i64 / 2)
            .bind(cfg.compact.old_data_min_files)
            .bind(include_lone_unindexed)
            .bind(cfg.compact.max_file_size_for_merge(stream_type, true) as i64 / 2)
            .fetch_all(&pool)
            .await?;
        Ok(ret
            .into_iter()
            .map(|r| r.try_get::<String, &str>("date").unwrap_or_default())
            .collect())
    }

    async fn query_deleted(
        &self,
        org_id: &str,
        time_max: i64,
        limit: i64,
    ) -> Result<Vec<FileListDeleted>> {
        if time_max == 0 {
            return Ok(Vec::new());
        }
        let pool = CLIENT_RO.clone();
        let ret = sqlx::query_as::<_, super::FileDeletedRecord>(
            r#"SELECT id, account, stream, date, file, index_file, flattened FROM file_list_deleted WHERE org = $1 AND created_at < $2 ORDER BY created_at ASC LIMIT $3;"#,
        )
        .bind(org_id)
        .bind(time_max)
        .bind(limit)
        .fetch_all(&pool)
        .await?;
        Ok(ret
            .iter()
            .map(|r| FileListDeleted {
                id: r.id,
                account: r.account.to_string(),
                file: format!("files/{}/{}/{}", r.stream, r.date, r.file),
                index_file: r.index_file,
                flattened: r.flattened,
            })
            .collect())
    }

    async fn list_deleted(&self) -> Result<Vec<FileListDeleted>> {
        let pool = CLIENT_RO.clone();
        let ret = sqlx::query_as::<_, super::FileDeletedRecord>(
            r#"SELECT id, account, stream, date, file, index_file, flattened FROM file_list_deleted;"#,
        )
        .fetch_all(&pool)
        .await?;
        Ok(ret
            .iter()
            .map(|r| FileListDeleted {
                id: r.id,
                account: r.account.to_string(),
                file: format!("files/{}/{}/{}", r.stream, r.date, r.file),
                index_file: r.index_file,
                flattened: r.flattened,
            })
            .collect())
    }

    async fn get_min_date(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        date_range: Option<(String, String)>,
    ) -> Result<String> {
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
        let pool = CLIENT_RO.clone();
        let ret: Option<String> = match date_range {
            Some((start, end)) => {
                sqlx::query_scalar(r#"SELECT MIN(date) AS num FROM file_list WHERE stream = $1 AND date >= $2 AND date < $3;"#)
                    .bind(stream_key)
                    .bind(start)
                    .bind(end)
                    .fetch_one(&pool)
                    .await?
            }
            None => {
                sqlx::query_scalar(r#"SELECT MIN(date) AS num FROM file_list WHERE stream = $1;"#)
                    .bind(stream_key)
                    .fetch_one(&pool)
                    .await?
            }
        };
        Ok(ret.unwrap_or_default())
    }

    async fn get_min_update_at(&self) -> Result<i64> {
        let pool = CLIENT_RO.clone();
        let ret: Option<i64> =
            sqlx::query_scalar(r#"SELECT MIN(updated_at) AS num FROM file_list;"#)
                .fetch_one(&pool)
                .await?;
        Ok(ret.unwrap_or_default())
    }

    async fn get_max_update_at(&self) -> Result<i64> {
        let pool = CLIENT_RO.clone();
        let ret: Option<i64> =
            sqlx::query_scalar(r#"SELECT MAX(updated_at) AS num FROM file_list;"#)
                .fetch_one(&pool)
                .await?;
        Ok(ret.unwrap_or_default())
    }

    async fn clean_by_min_update_at(&self, val: i64) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query("DELETE FROM file_list WHERE updated_at < $1;")
            .bind(val)
            .execute(&*client)
            .await?;
        Ok(())
    }

    async fn get_updated_streams(&self, time_range: (i64, i64)) -> Result<Vec<String>> {
        let (time_start, time_end) = time_range;
        let pool = CLIENT_RO.clone();
        let ret = sqlx::query(
            r#"SELECT DISTINCT stream FROM file_list WHERE updated_at > $1 AND updated_at <= $2;"#,
        )
        .bind(time_start)
        .bind(time_end)
        .fetch_all(&pool)
        .await?
        .into_iter()
        .map(|r| r.try_get::<String, &str>("stream").unwrap_or_default())
        .collect();
        Ok(ret)
    }

    async fn stats_by_date_range(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        date_range: (String, String),
    ) -> Result<StreamStats> {
        let (start_date, end_date) = date_range;
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
        let time_filter = if !start_date.is_empty() && !end_date.is_empty() {
            format!("AND date >= '{start_date}' AND date < '{end_date}'")
        } else if start_date.is_empty() && !end_date.is_empty() {
            format!("AND date < '{end_date}'")
        } else if !start_date.is_empty() && end_date.is_empty() {
            format!("AND date >= '{start_date}'")
        } else {
            "".to_string()
        };
        let sql = format!(
            r#"
SELECT 
    COUNT(*) AS file_num,
    MIN(min_ts) AS min_ts,
    MAX(max_ts) AS max_ts,
    SUM(records) AS records,
    SUM(original_size) AS original_size,
    SUM(compressed_size) AS compressed_size,
    SUM(index_size) AS index_size
FROM file_list
WHERE stream = $1 {time_filter}
GROUP BY stream;
            "#
        );
        let pool = CLIENT_RO.clone();
        let ret: Option<super::StatsRecord> = sqlx::query_as(&sql)
            .bind(stream_key)
            .fetch_optional(&pool)
            .await?;
        Ok(ret.map(|r| r.into()).unwrap_or_default())
    }

    async fn get_stream_stats(
        &self,
        org_id: &str,
        stream_type: Option<StreamType>,
        stream_name: Option<&str>,
    ) -> Result<Vec<(String, StreamStats)>> {
        // SECURITY: bind parameters to prevent SQL injection when any of the
        // inputs contain quotes or SQL metacharacters (GHSA-5x2v-jg9q-g8qc).
        let pool = CLIENT_RO.clone();
        let ret = if let Some(stream_type) = stream_type
            && let Some(stream_name) = stream_name
        {
            let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
            sqlx::query_as::<_, super::StatsRecord>("SELECT * FROM stream_stats WHERE stream = ?;")
                .bind(&stream_key)
                .fetch_all(&pool)
                .await?
        } else {
            sqlx::query_as::<_, super::StatsRecord>("SELECT * FROM stream_stats WHERE org = ?;")
                .bind(org_id)
                .fetch_all(&pool)
                .await?
        };
        let mut stats: HashMap<String, StreamStats> = HashMap::with_capacity(ret.len() / 2);
        for r in ret {
            match stats.get_mut(&r.stream) {
                Some(s) => s.merge(&r.into()),
                None => {
                    stats.insert(r.stream.to_owned(), r.into());
                }
            }
        }
        Ok(stats.into_iter().collect())
    }

    async fn del_stream_stats(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
    ) -> Result<()> {
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query("DELETE FROM stream_stats WHERE stream = ?;")
            .bind(&stream_key)
            .execute(&*client)
            .await?;
        Ok(())
    }

    async fn set_stream_stats(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        stats: &StreamStats,
        is_recent: bool,
    ) -> Result<()> {
        let stream_key = format!("{org_id}/{stream_type}/{stream_name}");
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await?;
        if let Err(e) = sqlx::query(
            r#"
INSERT INTO stream_stats
    (org, stream, file_num, min_ts, max_ts, records, original_size, compressed_size, index_size, is_recent)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (stream, is_recent)
DO UPDATE SET
    file_num = EXCLUDED.file_num,
    min_ts = EXCLUDED.min_ts,
    max_ts = EXCLUDED.max_ts,
    records = EXCLUDED.records,
    original_size = EXCLUDED.original_size,
    compressed_size = EXCLUDED.compressed_size,
    index_size = EXCLUDED.index_size;
            "#,
        )
        .bind(org_id)
        .bind(&stream_key)
        .bind(stats.file_num)
        .bind(stats.doc_time_min)
        .bind(stats.doc_time_max)
        .bind(stats.doc_num)
        .bind(stats.storage_size as i64)
        .bind(stats.compressed_size as i64)
        .bind(stats.index_size as i64)
        .bind(is_recent)
        .execute(&mut *tx)
        .await
        {
            if let Err(e) = tx.rollback().await {
                log::error!("[SQLITE] rollback set stream stats error: {e}");
            }
            return Err(e.into());
        }

        // commit
        if let Err(e) = tx.commit().await {
            log::error!("[SQLITE] commit set stream stats error: {e}");
            return Err(e.into());
        }

        Ok(())
    }

    async fn reset_stream_stats(&self) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query(r#"UPDATE stream_stats SET file_num = 0, min_ts = 0, max_ts = 0, records = 0, original_size = 0, compressed_size = 0, index_size = 0;"#)
        .execute(&*client)
       .await?;
        Ok(())
    }

    async fn len(&self) -> usize {
        let pool = CLIENT_RO.clone();
        let ret = match sqlx::query(r#"SELECT COUNT(*) as num FROM file_list;"#)
            .fetch_one(&pool)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                log::error!("[SQLITE] get file list len error: {e}");
                return 0;
            }
        };
        match ret.try_get::<i64, &str>("num") {
            Ok(v) => v as usize,
            _ => 0,
        }
    }

    async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    async fn clear(&self) -> Result<()> {
        Ok(())
    }

    async fn add_job(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream: &str,
        offset: i64,
    ) -> Result<i64> {
        let stream_key = format!("{org_id}/{stream_type}/{stream}");
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await?;
        // A trigger that collides with an owned merge or dump must survive
        // that worker's snapshot. Unowned DONE rows are resurrected
        // immediately. Neither conflict path changes the active lease.
        match sqlx::query(
            r#"INSERT INTO file_list_jobs
    (org, stream, offsets, status, node, started_at, updated_at)
VALUES ($1, $2, $3, $4, '', 0, 0)
ON CONFLICT (stream, offsets) DO UPDATE SET
    status = CASE
        WHEN file_list_jobs.status = $6 AND file_list_jobs.node = '' THEN $4
        ELSE file_list_jobs.status
    END,
    started_at = CASE
        WHEN file_list_jobs.status = $6 AND file_list_jobs.node = '' THEN 0
        ELSE file_list_jobs.started_at
    END,
    pending_after_run = CASE
        WHEN file_list_jobs.status = $5 THEN true
        WHEN file_list_jobs.status = $6 AND file_list_jobs.node = '' THEN false
        ELSE file_list_jobs.pending_after_run
    END,
    pending_after_dump = CASE
        WHEN file_list_jobs.status = $6 AND file_list_jobs.node = '' THEN false
        WHEN file_list_jobs.status = $6 THEN true
        ELSE file_list_jobs.pending_after_dump
    END
WHERE file_list_jobs.status IN ($5, $6);"#,
        )
        .bind(org_id)
        .bind(&stream_key)
        .bind(offset)
        .bind(super::FileListJobStatus::Pending)
        .bind(super::FileListJobStatus::Running)
        .bind(super::FileListJobStatus::Done)
        .execute(&mut *tx)
        .await
        {
            Err(sqlx::Error::Database(e)) => {
                if !e.is_unique_violation() {
                    return Err(Error::Message(e.to_string()));
                }
            }
            Err(e) => {
                return Err(e.into());
            }
            Ok(_) => {}
        };

        // get job id
        let ret = match sqlx::query(
            "SELECT id, status FROM file_list_jobs WHERE org = $1 AND stream = $2 AND offsets = $3;",
        )
        .bind(org_id)
        .bind(&stream_key)
        .bind(offset)
        .fetch_one(&mut *tx)
        .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(e) = tx.rollback().await {
                    log::error!("[SQLITE] rollback add job error: {e}");
                }
                return Err(e.into());
            }
        };
        let id = ret.try_get::<i64, &str>("id").unwrap_or_default();
        let status = ret.try_get::<i64, &str>("status").unwrap_or_default();
        if id > 0
            && super::FileListJobStatus::from(status) == super::FileListJobStatus::Done
            && let Err(e) = sqlx::query(
                "UPDATE file_list_jobs SET status = $1, node = '', started_at = 0, \
                 pending_after_run = false, pending_after_dump = false \
                 WHERE status = $2 AND node = '' AND id = $3;",
            )
            .bind(super::FileListJobStatus::Pending)
            .bind(super::FileListJobStatus::Done)
            .bind(id)
            .execute(&mut *tx)
            .await
        {
            if let Err(e) = tx.rollback().await {
                log::error!("[SQLITE] rollback update job status error: {e}");
            }
            return Err(e.into());
        }
        if let Err(e) = tx.commit().await {
            log::error!("[SQLITE] commit add job error: {e}");
            return Err(e.into());
        }
        Ok(id)
    }

    async fn get_pending_jobs(
        &self,
        node: &str,
        limit: i64,
        order: super::FileListJobOrder,
        min_offsets: Option<i64>,
        max_offsets: Option<i64>,
    ) -> Result<Vec<super::MergeJobRecord>> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await?;
        let order_sql = match order {
            // DONE debt rows are resurrected in place, retaining their old
            // ids. Completion/requeue stamps updated_at, so it is the true
            // FIFO enqueue clock; id only breaks equal-time ties.
            super::FileListJobOrder::EnqueueOldest => "updated_at ASC, id ASC",
            // Fast mode still rotates work within the selected hour.
            super::FileListJobOrder::OffsetNewest => "offsets DESC, updated_at ASC, id ASC",
        };
        let sql = format!(
            r#"UPDATE file_list_jobs
SET status = $1,
    node = $2,
    started_at = $3,
    updated_at = $3,
    pending_after_run = false,
    lease_generation = lease_generation + 1
WHERE id IN (
    SELECT id
    FROM file_list_jobs
    WHERE status = $4
      AND ($5 IS NULL OR offsets >= $5)
      AND ($6 IS NULL OR offsets < $6)
    ORDER BY {order_sql}
    LIMIT $7
)
RETURNING id, stream, offsets, lease_generation;"#
        );
        let now = config::utils::time::now_micros();
        let mut ret = match sqlx::query_as::<_, super::MergeJobRecord>(&sql)
            .bind(super::FileListJobStatus::Running)
            .bind(node)
            .bind(now)
            .bind(super::FileListJobStatus::Pending)
            .bind(min_offsets)
            .bind(max_offsets)
            .bind(limit)
            .fetch_all(&mut *tx)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(rollback_error) = tx.rollback().await {
                    log::error!("[SQLITE] rollback get_pending_jobs error: {rollback_error}");
                }
                return Err(e.into());
            }
        };
        tx.commit().await?;
        match order {
            super::FileListJobOrder::EnqueueOldest => ret.sort_unstable_by_key(|r| r.id),
            super::FileListJobOrder::OffsetNewest => {
                ret.sort_unstable_by_key(|r| (std::cmp::Reverse(r.offsets), r.id));
            }
        }
        Ok(ret)
    }

    async fn reset_jobs_admin(&self, offsets: i64, stream: Option<&str>) -> Result<u64> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET status = $1,
    node = '',
    updated_at = $4,
    pending_after_run = false,
    pending_after_dump = false,
    lease_generation = lease_generation + 1
WHERE ($2 <= 0 OR offsets >= $2)
  AND ($3 IS NULL OR stream = $3);"#,
        )
        .bind(super::FileListJobStatus::Pending)
        .bind(offsets)
        .bind(stream)
        .bind(now_micros())
        .execute(&*client)
        .await?;
        Ok(ret.rows_affected())
    }

    async fn touch_job_lease(
        &self,
        id: i64,
        node: &str,
        generation: i64,
        expected_status: super::FileListJobStatus,
    ) -> Result<bool> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET updated_at = $1
WHERE id = $2 AND node = $3 AND lease_generation = $4 AND status = $5;"#,
        )
        .bind(now_micros())
        .bind(id)
        .bind(node)
        .bind(generation)
        .bind(expected_status)
        .execute(&*client)
        .await?;
        Ok(ret.rows_affected() == 1)
    }

    async fn set_job_pending_owned(&self, id: i64, node: &str, generation: i64) -> Result<bool> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET status = $1, node = '', updated_at = $2, pending_after_dump = false
WHERE id = $3 AND node = $4 AND lease_generation = $5 AND status = $6;"#,
        )
        .bind(super::FileListJobStatus::Pending)
        .bind(now_micros())
        .bind(id)
        .bind(node)
        .bind(generation)
        .bind(super::FileListJobStatus::Running)
        .execute(&*client)
        .await?;
        Ok(ret.rows_affected() == 1)
    }

    async fn set_job_done_owned(&self, id: i64, node: &str, generation: i64) -> Result<bool> {
        let cfg = get_config();
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET status = CASE WHEN pending_after_run THEN $1 ELSE $2 END,
    started_at = CASE WHEN pending_after_run THEN 0 ELSE started_at END,
    updated_at = $3,
    dumped = CASE WHEN pending_after_run THEN false ELSE $4 END,
    node = '',
    pending_after_dump = CASE WHEN pending_after_run THEN pending_after_dump ELSE false END,
    pending_after_run = false
WHERE id = $5 AND node = $6 AND lease_generation = $7 AND status = $8;"#,
        )
        .bind(super::FileListJobStatus::Pending)
        .bind(super::FileListJobStatus::Done)
        .bind(now_micros())
        .bind(!cfg.compact.file_list_dump_enabled)
        .bind(id)
        .bind(node)
        .bind(generation)
        .bind(super::FileListJobStatus::Running)
        .execute(&*client)
        .await?;
        Ok(ret.rows_affected() == 1)
    }

    async fn check_running_jobs(&self, before_date: i64) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;

        // Resetting is itself a fencing event, not merely a status change.
        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET status = $1, node = '', lease_generation = lease_generation + 1
WHERE status = $2 AND updated_at < $3;"#,
        )
        .bind(super::FileListJobStatus::Pending)
        .bind(super::FileListJobStatus::Running)
        .bind(before_date)
        .execute(&*client)
        .await?;
        let rows_affected = ret.rows_affected();
        if rows_affected > 0 {
            log::warn!(
                "[SQLITE] reset running jobs status to pending, rows_affected: {rows_affected}"
            );
        }

        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET node = '', lease_generation = lease_generation + 1
WHERE status = $1 AND dumped = $2 AND node != '' AND updated_at < $3;"#,
        )
        .bind(super::FileListJobStatus::Done)
        .bind(false)
        .bind(before_date)
        .execute(&*client)
        .await?;
        let rows_affected = ret.rows_affected();
        if rows_affected > 0 {
            log::warn!("[SQLITE] reset dumping jobs node to empty, rows_affected: {rows_affected}");
        }
        Ok(())
    }

    async fn clean_done_jobs(&self, before_date: i64) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"DELETE FROM file_list_jobs WHERE status = $1 AND dumped = $2 AND updated_at < $3;"#,
        )
        .bind(super::FileListJobStatus::Done)
        .bind(true)
        .bind(before_date)
        .execute(&*client)
        .await?;
        if ret.rows_affected() > 0 {
            log::warn!("[SQLITE] clean done jobs");
        }
        Ok(())
    }

    async fn get_pending_jobs_count(&self) -> Result<stdHashMap<String, stdHashMap<String, i64>>> {
        let pool = CLIENT_RO.clone();

        let ret =
            sqlx::query(r#"SELECT stream, status, count(*) as counts FROM file_list_jobs WHERE status = $1 GROUP BY stream, status ORDER BY status desc;"#)
                .bind(super::FileListJobStatus::Pending)
                .fetch_all(&pool)
                .await?;

        let mut job_status: stdHashMap<String, stdHashMap<String, i64>> = stdHashMap::new();

        for r in ret.iter() {
            let stream = r.get::<String, &str>("stream");
            let status = r.get::<i32, &str>("status");
            let counts = if status == 0 {
                r.get::<i64, &str>("counts")
            } else {
                0
            };
            let parts: Vec<&str> = stream.split('/').collect();
            if parts.len() >= 2 {
                let org = parts[0].to_string();
                let stream_type = parts[1].to_string();
                job_status
                    .entry(org)
                    .or_default()
                    .entry(stream_type)
                    .and_modify(|e| *e += counts)
                    .or_insert(counts);
            }
        }
        Ok(job_status)
    }

    async fn get_pending_dump_jobs(
        &self,
        node: &str,
        limit: i64,
    ) -> Result<Vec<super::DumpJobRecord>> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await?;
        let now = config::utils::time::now_micros();
        let mut ret = match sqlx::query_as::<_, super::DumpJobRecord>(
            r#"UPDATE file_list_jobs
SET node = $1,
    started_at = $2,
    updated_at = $2,
    lease_generation = lease_generation + 1
WHERE id IN (
    SELECT id
    FROM file_list_jobs
    WHERE status = $3 AND dumped = $4 AND node = ''
    ORDER BY updated_at ASC, id ASC
    LIMIT $5
)
RETURNING id, stream, offsets, lease_generation;"#,
        )
        .bind(node)
        .bind(now)
        .bind(super::FileListJobStatus::Done)
        .bind(false)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        {
            Ok(v) => v,
            Err(e) => {
                if let Err(rollback_error) = tx.rollback().await {
                    log::error!("[SQLITE] rollback get_pending_dump_jobs error: {rollback_error}");
                }
                return Err(e.into());
            }
        };
        tx.commit().await?;
        ret.sort_unstable_by_key(|r| r.id);
        Ok(ret)
    }

    async fn set_job_dumped_status_owned(
        &self,
        id: i64,
        node: &str,
        generation: i64,
        dumped: bool,
    ) -> Result<bool> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
            r#"UPDATE file_list_jobs
SET status = CASE WHEN $1 AND pending_after_dump THEN $2 ELSE status END,
    dumped = CASE WHEN $1 AND pending_after_dump THEN false ELSE $1 END,
    node = '',
    updated_at = $3,
    pending_after_dump = CASE WHEN $1 AND pending_after_dump THEN false ELSE pending_after_dump END
WHERE id = $4 AND node = $5 AND lease_generation = $6 AND status = $7;"#,
        )
        .bind(dumped)
        .bind(super::FileListJobStatus::Pending)
        .bind(now_micros())
        .bind(id)
        .bind(node)
        .bind(generation)
        .bind(super::FileListJobStatus::Done)
        .execute(&*client)
        .await?;
        Ok(ret.rows_affected() == 1)
    }

    async fn insert_dump_stats(&self, file: &str, stats: &StreamStats) -> Result<()> {
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).expect("parse file key failed");
        let org_id = stream_key[..stream_key.find('/').unwrap()].to_string();
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query(
            r#"
INSERT INTO file_list_dump_stats
    (org, stream, date, file, file_num, min_ts, max_ts, records, original_size, compressed_size, index_size)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (stream, date, file)
DO UPDATE SET
    file_num = EXCLUDED.file_num,
    min_ts = EXCLUDED.min_ts,
    max_ts = EXCLUDED.max_ts,
    records = EXCLUDED.records,
    original_size = EXCLUDED.original_size,
    compressed_size = EXCLUDED.compressed_size,
    index_size = EXCLUDED.index_size;
            "#,
        )
        .bind(org_id)
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .bind(stats.file_num)
        .bind(stats.doc_time_min)
        .bind(stats.doc_time_max)
        .bind(stats.doc_num)
        .bind(stats.storage_size as i64)
        .bind(stats.compressed_size as i64)
        .bind(stats.index_size as i64)
        .execute(&*client)
        .await?;
        Ok(())
    }

    async fn delete_dump_stats(&self, file: &str) -> Result<()> {
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).expect("parse file key failed");
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query(
            r#"DELETE FROM file_list_dump_stats WHERE stream = $1 AND date = $2 AND file = $3;"#,
        )
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .execute(&*client)
        .await?;
        Ok(())
    }

    async fn query_dump_stats_by_date_range(
        &self,
        org_id: &str,
        stream_type: StreamType,
        stream_name: &str,
        date_range: (String, String),
    ) -> Result<StreamStats> {
        let (start_date, end_date) = date_range;
        let stream_key = format!(
            "{org_id}/{}/{stream_name}_{stream_type}",
            StreamType::Filelist
        );
        let time_filter = if !start_date.is_empty() && !end_date.is_empty() {
            format!("AND date >= '{start_date}' AND date < '{end_date}'")
        } else if start_date.is_empty() && !end_date.is_empty() {
            format!("AND date < '{end_date}'")
        } else if !start_date.is_empty() && end_date.is_empty() {
            format!("AND date >= '{start_date}'")
        } else {
            "".to_string()
        };
        let sql = format!(
            r#"
SELECT 
    SUM(file_num) AS file_num,
    MIN(min_ts) AS min_ts,
    MAX(max_ts) AS max_ts,
    SUM(records) AS records,
    SUM(original_size) AS original_size,
    SUM(compressed_size) AS compressed_size,
    SUM(index_size) AS index_size
FROM file_list_dump_stats
WHERE stream = $1 {time_filter}
GROUP BY stream;
            "#
        );
        let pool = CLIENT_RO.clone();
        let ret: Option<super::StatsRecord> = sqlx::query_as(&sql)
            .bind(stream_key)
            .fetch_optional(&pool)
            .await?;
        Ok(ret.map(|r| r.into()).unwrap_or_default())
    }

    async fn org_stats_by_account(&self, org_id: &str, account: &str) -> Result<(i64, i64)> {
        let sql = r#"SELECT
SUM(original_size) AS original_size,
SUM(index_size) AS index_size
FROM file_list
WHERE org = $1 AND account = $2;"#;
        let pool = CLIENT_RO.clone();
        let ret: Option<(i64, i64)> = sqlx::query_as(sql)
            .bind(org_id)
            .bind(account)
            .fetch_optional(&pool)
            .await?;
        Ok(ret.unwrap_or_default())
    }

    async fn delete_by_org(&self, org_id: &str) -> Result<()> {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let created_at = now_micros();
        let mut tx = client.begin().await?;
        // Move remaining rows into file_list_deleted first so the file GC removes
        // the backing S3 objects. A bare DELETE would orphan those files in object
        // store. (Normal per-stream deletion already routes files here; this is the
        // catch-all for rows whose stream schema is already gone.)
        sqlx::query(super::MOVE_FILE_LIST_TO_DELETED_SQL)
            .bind(org_id)
            .bind(created_at)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM file_list WHERE org = $1;")
            .bind(org_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl SqliteFileList {
    async fn inner_add(
        &self,
        table: &str,
        account: &str,
        file: &str,
        meta: &FileMeta,
    ) -> Result<i64> {
        self.inner_add_with_id(table, None, account, file, meta)
            .await
    }

    async fn inner_add_with_id(
        &self,
        table: &str,
        id: Option<i64>,
        account: &str,
        file: &str,
        meta: &FileMeta,
    ) -> Result<i64> {
        super::validate_file_meta_for_add(file, meta)?;
        let now_ts = now_micros();
        let (stream_key, date_key, file_name) =
            parse_file_key_columns(file).map_err(|e| Error::Message(e.to_string()))?;
        let org_id = stream_key[..stream_key.find('/').unwrap()].to_string();
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        match  sqlx::query(
            format!(r#"
INSERT INTO {table} (id, account, org, stream, date, file, deleted, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);
        "#).as_str(),
    )
        .bind(id)
        .bind(account)
        .bind(org_id)
        .bind(stream_key)
        .bind(date_key)
        .bind(file_name)
        .bind(false)
        .bind(meta.min_ts)
        .bind(meta.max_ts)
        .bind(meta.records)
        .bind(meta.original_size)
        .bind(meta.compressed_size)
        .bind(meta.index_size)
        .bind(meta.bloom_ver)
        .bind(meta.flattened)
        .bind(now_ts)
        .execute(&*client)
        .await {
            Err(sqlx::Error::Database(e)) => if e.is_unique_violation() {
                  Ok(0)
            } else {
                  Err(Error::Message(e.to_string()))
            },
            Err(e) =>  Err(e.into()),
            Ok(v) => Ok(v.last_insert_rowid()),
        }
    }

    async fn inner_batch_process(&self, table: &str, files: &[FileKey]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        // pre-SQL gate: see `prepare_batch_add` — one bad add fails the
        // whole batch before the writer lock or any statement
        let add_rows = super::prepare_batch_add(files)?;

        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let mut tx = client.begin().await?;

        if let Err(e) = batch_add_with_tx(&mut tx, table, &add_rows).await {
            if let Err(e) = tx.rollback().await {
                log::error!("[SQLITE] rollback {table} batch process for add error: {e}");
            }
            return Err(e);
        }

        // sort by file id and key to reduce locked table range
        let mut del_items = files.iter().filter(|v| v.deleted).collect::<Vec<_>>();
        del_items.sort_by(|v1, v2| match v1.id.cmp(&v2.id) {
            std::cmp::Ordering::Equal => v1.key.cmp(&v2.key),
            other => other,
        });
        let deleted_batch_size = get_config().compact.file_list_deleted_batch_size;
        if !del_items.is_empty() {
            let chunks = del_items.chunks(deleted_batch_size);
            for files in chunks {
                // get ids of the files
                let mut ids = Vec::with_capacity(files.len());
                for file in files {
                    if file.id > 0 {
                        ids.push(file.id.to_string());
                        continue;
                    }
                    let (stream_key, date_key, file_name) = parse_file_key_columns(&file.key)
                        .map_err(|e| Error::Message(e.to_string()))?;
                    let query_res: std::result::Result<Option<i64>, sea_orm::SqlxError> = sqlx::query_scalar(
                    r#"SELECT id FROM file_list WHERE stream = $1 AND date = $2 AND file = $3;"#,
                    )
                    .bind(stream_key)
                    .bind(date_key)
                    .bind(file_name)
                    .fetch_one(&mut *tx)
                    .await;
                    match query_res {
                        Ok(Some(v)) => ids.push(v.to_string()),
                        Ok(None) => continue,
                        Err(sqlx::Error::RowNotFound) => continue,
                        Err(e) => {
                            if let Err(e) = tx.rollback().await {
                                log::error!(
                                    "[SQLITE] rollback {table} batch process for delete error: {e}"
                                );
                            }
                            return Err(e.into());
                        }
                    };
                }
                // delete files by ids
                if !ids.is_empty() {
                    let sql = format!("DELETE FROM file_list WHERE id IN({});", ids.join(","));
                    if let Err(e) = sqlx::query(sql.as_str()).execute(&mut *tx).await {
                        if let Err(e) = tx.rollback().await {
                            log::error!(
                                "[SQLITE] rollback {table} batch process for delete error: {e}"
                            );
                        }
                        return Err(e.into());
                    }
                }
            }
        }

        if let Err(e) = tx.commit().await {
            log::error!("[SQLITE] commit {table} batch process error: {e}");
            return Err(e.into());
        }

        // release lock
        drop(client);

        Ok(())
    }
}

/// Add-side of the batch transaction, shared by `inner_batch_process` and
/// `wal_segments::mark_built_with_files` so the INSERT stays single-source:
/// chunked VALUES insert of pre-validated rows (`prepare_batch_add`) inside
/// the caller's open transaction. Takes NO lock — the caller must already
/// hold `CLIENT_RW` (the single writer) — and owns commit/rollback.
/// `ON CONFLICT(id)` only tolerates id collisions; a duplicate
/// `(stream, date, file)` errors and fails the caller's whole transaction.
pub(crate) async fn batch_add_with_tx(
    tx: &mut sqlx::Transaction<'_, Sqlite>,
    table: &str,
    rows: &[super::BatchAddRow<'_>],
) -> Result<()> {
    for chunk in rows.chunks(100) {
        let now_ts = now_micros();
        let mut query_builder: QueryBuilder<Sqlite> = QueryBuilder::new(
            format!("INSERT INTO {table} (id, account, org, stream, date, file, deleted, min_ts, max_ts, records, original_size, compressed_size, index_size, bloom_ver, flattened, updated_at)").as_str(),
        );
        query_builder.push_values(
            chunk,
            |mut b, (item, org_id, stream_key, date_key, file_name)| {
                let id = if item.id > 0 { Some(item.id) } else { None };
                b.push_bind(id)
                    .push_bind(&item.account)
                    .push_bind(org_id)
                    .push_bind(stream_key)
                    .push_bind(date_key)
                    .push_bind(file_name)
                    .push_bind(false)
                    .push_bind(item.meta.min_ts)
                    .push_bind(item.meta.max_ts)
                    .push_bind(item.meta.records)
                    .push_bind(item.meta.original_size)
                    .push_bind(item.meta.compressed_size)
                    .push_bind(item.meta.index_size)
                    .push_bind(item.meta.bloom_ver)
                    .push_bind(item.meta.flattened)
                    .push_bind(now_ts);
            },
        );
        query_builder.push(" ON CONFLICT(id) DO NOTHING");
        query_builder.build().execute(&mut **tx).await?;
    }
    Ok(())
}

pub async fn create_table() -> Result<()> {
    let client = CLIENT_RW.clone();
    let client = client.lock().await;
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS file_list
(
    id        INTEGER not null primary key autoincrement,
    account   VARCHAR not null,
    org       VARCHAR not null,
    stream    VARCHAR not null,
    date      VARCHAR not null,
    file      VARCHAR not null,
    deleted   BOOLEAN default false not null,
    flattened BOOLEAN default false not null,
    min_ts    BIGINT not null,
    max_ts    BIGINT not null,
    records   BIGINT not null,
    original_size   BIGINT not null,
    compressed_size BIGINT not null,
    index_size      BIGINT not null,
    bloom_ver       BIGINT default 0 not null,
    updated_at      BIGINT not null
);
        "#,
    )
    .execute(&*client)
    .await?;

    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS file_list_history
(
    id        INTEGER not null primary key autoincrement,
    account   VARCHAR not null,
    org       VARCHAR not null,
    stream    VARCHAR not null,
    date      VARCHAR not null,
    file      VARCHAR not null,
    deleted   BOOLEAN default false not null,
    flattened BOOLEAN default false not null,
    min_ts    BIGINT not null,
    max_ts    BIGINT not null,
    records   BIGINT not null,
    original_size   BIGINT not null,
    compressed_size BIGINT not null,
    index_size      BIGINT not null,
    bloom_ver       BIGINT default 0 not null,
    updated_at      BIGINT not null
);
        "#,
    )
    .execute(&*client)
    .await?;

    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS file_list_deleted
(
    id         INTEGER not null primary key autoincrement,
    account    VARCHAR not null,
    org        VARCHAR not null,
    stream     VARCHAR not null,
    date       VARCHAR not null,
    file       VARCHAR not null,
    index_file BOOLEAN default false not null,
    flattened  BOOLEAN default false not null,
    created_at BIGINT not null
);
        "#,
    )
    .execute(&*client)
    .await?;

    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS file_list_jobs
(
    id         INTEGER not null primary key autoincrement,
    org        VARCHAR not null,
    stream     VARCHAR not null,
    offsets    BIGINT not null,
    status     INT not null,
    node       VARCHAR not null,
    started_at BIGINT not null,
    updated_at BIGINT not null,
    dumped     BOOLEAN default false not null,
    lease_generation BIGINT default 0 not null,
    pending_after_run BOOLEAN default false not null,
    pending_after_dump BOOLEAN default false not null
);
        "#,
    )
    .execute(&*client)
    .await?;

    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS stream_stats
(
    id      INTEGER not null primary key autoincrement,
    org     VARCHAR not null,
    stream  VARCHAR not null,
    file_num BIGINT not null,
    min_ts   BIGINT not null,
    max_ts   BIGINT not null,
    records  BIGINT not null,
    original_size   BIGINT not null,
    compressed_size BIGINT not null,
    index_size      BIGINT not null,
    is_recent       BOOLEAN default false not null
);
        "#,
    )
    .execute(&*client)
    .await?;

    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS file_list_dump_stats
(
    id              INTEGER not null primary key autoincrement,
    org             VARCHAR not null,
    stream          VARCHAR not null,
    date            VARCHAR not null,
    file            VARCHAR not null,
    file_num        BIGINT default 0 not null,
    min_ts          BIGINT default 0 not null,
    max_ts          BIGINT default 0 not null,
    records         BIGINT default 0 not null,
    original_size   BIGINT default 0 not null,
    compressed_size BIGINT default 0 not null,
    index_size      BIGINT default 0 not null
);
        "#,
    )
    .execute(&*client)
    .await?;

    // create column flattened for old version <= 0.10.5
    let column = "flattened";
    let data_type = "BOOLEAN default false not null";
    add_column(&client, "file_list", column, data_type).await?;
    add_column(&client, "file_list_history", column, data_type).await?;
    add_column(&client, "file_list_deleted", column, data_type).await?;

    // create column started_at for old version <= 0.10.8
    let column = "started_at";
    let data_type = "BIGINT default 0 not null";
    add_column(&client, "file_list_jobs", column, data_type).await?;

    // create column index_size for old version <= 0.13.1
    let column = "index_size";
    let data_type = "BIGINT default 0 not null";
    add_column(&client, "file_list", column, data_type).await?;
    add_column(&client, "file_list_history", column, data_type).await?;
    add_column(&client, "stream_stats", column, data_type).await?;
    let column = "index_file";
    let data_type = "BOOLEAN default false not null";
    add_column(&client, "file_list_deleted", column, data_type).await?;

    // create col dumped for file_list_jobs for version <=0.14.0
    add_column(
        &client,
        "file_list_jobs",
        "dumped",
        "BOOLEAN default false not null",
    )
    .await?;

    // create col account for multiple object storage account support, version >= 0.14.6
    add_column(
        &client,
        "file_list",
        "account",
        "VARCHAR(128) default '' not null",
    )
    .await?;
    add_column(
        &client,
        "file_list_history",
        "account",
        "VARCHAR(128) default '' not null",
    )
    .await?;
    add_column(
        &client,
        "file_list_deleted",
        "account",
        "VARCHAR(128) default '' not null",
    )
    .await?;

    // create column updated_at for version >= 0.14.7
    let column = "updated_at";
    let data_type = "BIGINT default 0 not null";
    add_column(&client, "file_list", column, data_type).await?;
    add_column(&client, "file_list_history", column, data_type).await?;

    // create column bloom_ver for bloom filter pruning above the inverted index
    let column = "bloom_ver";
    let data_type = "BIGINT default 0 not null";
    add_column(&client, "file_list", column, data_type).await?;
    add_column(&client, "file_list_history", column, data_type).await?;

    add_column(
        &client,
        "file_list_jobs",
        "lease_generation",
        "BIGINT default 0 not null",
    )
    .await?;
    add_column(
        &client,
        "file_list_jobs",
        "pending_after_run",
        "BOOLEAN default false not null",
    )
    .await?;
    add_column(
        &client,
        "file_list_jobs",
        "pending_after_dump",
        "BOOLEAN default false not null",
    )
    .await?;
    // SQLite databases are normally node-local, but keep the same mixed-version
    // handoff contract if an old process overlaps an upgraded process on one
    // file. These triggers predate this behavior and were originally created
    // with IF NOT EXISTS, so recreate them on every upgrade.
    let mut trigger_tx = client.begin().await?;
    sqlx::query("DROP TRIGGER IF EXISTS file_list_jobs_pending_after_run_transition;")
        .execute(&mut *trigger_tx)
        .await?;
    sqlx::query(
        r#"
CREATE TRIGGER file_list_jobs_pending_after_run_transition
AFTER UPDATE OF status, dumped ON file_list_jobs
BEGIN
    UPDATE file_list_jobs
    SET pending_after_run = false
    WHERE id = NEW.id
      AND OLD.status = 0
      AND NEW.status = 1
      AND OLD.pending_after_run;
    UPDATE file_list_jobs
    SET status = 0,
        node = '',
        started_at = 0,
        dumped = false,
        pending_after_run = false
    WHERE id = NEW.id
      AND OLD.status IN (0, 1)
      AND NEW.status = 2
      AND OLD.pending_after_run;
    UPDATE file_list_jobs
    SET status = 0,
        node = '',
        started_at = 0,
        dumped = false,
        pending_after_dump = false
    WHERE id = NEW.id
      AND OLD.status = 2
      AND OLD.node <> ''
      AND OLD.pending_after_dump
      AND NEW.dumped;
END;
"#,
    )
    .execute(&mut *trigger_tx)
    .await?;
    // An exact legacy add uses an INSERT whose conflict update would steal an
    // owned DONE dump lease. Intercept active merge and dump conflicts, latch
    // the corresponding rerun, and suppress only the duplicate insert.
    sqlx::query("DROP TRIGGER IF EXISTS file_list_jobs_legacy_insert_running_latch;")
        .execute(&mut *trigger_tx)
        .await?;
    sqlx::query(
        r#"
CREATE TRIGGER file_list_jobs_legacy_insert_running_latch
BEFORE INSERT ON file_list_jobs
WHEN NEW.status = 0
 AND NEW.node = ''
 AND NEW.started_at = 0
 AND NEW.updated_at = 0
 AND NEW.dumped = false
 AND NEW.lease_generation = 0
 AND NEW.pending_after_run = false
 AND NEW.pending_after_dump = false
 AND EXISTS (
     SELECT 1
     FROM file_list_jobs
     WHERE stream = NEW.stream
       AND offsets = NEW.offsets
       AND (status = 1 OR (status = 2 AND node <> ''))
 )
BEGIN
    UPDATE file_list_jobs
    SET pending_after_run = true
    WHERE stream = NEW.stream
      AND offsets = NEW.offsets
      AND status = 1;
    UPDATE file_list_jobs
    SET pending_after_dump = true
    WHERE stream = NEW.stream
      AND offsets = NEW.offsets
      AND status = 2
      AND node <> '';
    SELECT RAISE(IGNORE);
END;
"#,
    )
    .execute(&mut *trigger_tx)
    .await?;
    trigger_tx.commit().await?;

    // create columns is_recent and updated_at for stream_stats for version >= 0.30.0
    add_column(
        &client,
        "stream_stats",
        "is_recent",
        "BOOLEAN default false not null",
    )
    .await?;

    // removed created_at column for version <= 0.60.0
    drop_column(&client, "file_list", "created_at").await?;
    drop_column(&client, "file_list_history", "created_at").await?;

    Ok(())
}

pub async fn create_table_index() -> Result<()> {
    let indices: Vec<(&str, &str, &[&str])> = vec![
        ("file_list_org_idx", "file_list", &["org"]),
        (
            "file_list_stream_ts_idx",
            "file_list",
            &["stream", "max_ts", "min_ts"],
        ),
        (
            "file_list_stream_date_idx",
            "file_list",
            &["stream", "date"],
        ),
        (
            "file_list_updated_at_deleted_idx",
            "file_list",
            &["updated_at", "deleted"],
        ),
        ("file_list_history_org_idx", "file_list_history", &["org"]),
        (
            "file_list_history_stream_ts_idx",
            "file_list_history",
            &["stream", "max_ts", "min_ts"],
        ),
        (
            "file_list_deleted_created_at_idx",
            "file_list_deleted",
            &["org", "created_at"],
        ),
        (
            "file_list_deleted_stream_date_file_idx",
            "file_list_deleted",
            &["stream", "date", "file"],
        ),
        (
            "file_list_jobs_stream_status_idx",
            "file_list_jobs",
            &["status", "stream"],
        ),
        (
            "file_list_jobs_status_dumped_idx",
            "file_list_jobs",
            &["status", "dumped"],
        ),
        ("stream_stats_org_idx", "stream_stats", &["org"]),
        (
            "file_list_dump_stats_org_idx",
            "file_list_dump_stats",
            &["org"],
        ),
    ];
    for (idx, table, fields) in indices {
        create_index(IndexStatement::new(idx, table, false, fields)).await?;
    }

    let unique_indices: Vec<(&str, &str, &[&str])> = vec![
        (
            "file_list_history_stream_file_idx",
            "file_list_history",
            &["stream", "date", "file"],
        ),
        (
            "file_list_jobs_stream_offsets_idx",
            "file_list_jobs",
            &["stream", "offsets"],
        ),
        (
            "stream_stats_org_stream_recent_idx",
            "stream_stats",
            &["stream", "is_recent"],
        ),
        (
            "file_list_dump_stats_stream_file_idx",
            "file_list_dump_stats",
            &["stream", "date", "file"],
        ),
    ];
    for (idx, table, fields) in unique_indices {
        create_index(IndexStatement::new(idx, table, true, fields)).await?;
    }

    // This is a case where we want to MAKE the index unique
    let res = create_index(IndexStatement::new(
        "file_list_stream_file_idx",
        "file_list",
        true,
        &["stream", "date", "file"],
    ))
    .await;
    if let Err(e) = res {
        if !e.to_string().contains("UNIQUE constraint failed") {
            return Err(e);
        }
        // delete duplicate records
        log::warn!("[SQLITE] starting delete duplicate records");
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ret = sqlx::query(
                r#"SELECT stream, date, file, min(id) as id FROM file_list GROUP BY stream, date, file HAVING COUNT(*) > 1;"#,
            ).fetch_all(&*client).await?;
        log::warn!("[SQLITE] total: {} duplicate records", ret.len());
        for (i, r) in ret.iter().enumerate() {
            let stream = r.get::<String, &str>("stream");
            let date = r.get::<String, &str>("date");
            let file = r.get::<String, &str>("file");
            let id = r.get::<i64, &str>("id");
            sqlx::query(
                    r#"DELETE FROM file_list WHERE id != $1 AND stream = $2 AND date = $3 AND file = $4;"#,
                ).bind(id).bind(stream).bind(date).bind(file).execute(&*client).await?;
            if i.is_multiple_of(1000) {
                log::warn!("[SQLITE] delete duplicate records: {}/{}", i, ret.len());
            }
        }
        drop(client);
        log::warn!(
            "[SQLITE] delete duplicate records: {}/{}",
            ret.len(),
            ret.len()
        );
        // create index again
        create_index(IndexStatement::new(
            "file_list_stream_file_idx",
            "file_list",
            true,
            &["stream", "date", "file"],
        ))
        .await?;
        log::warn!("[SQLITE] create table index(file_list_stream_file_idx) successfully");
    }

    // delete old index stream_stats_stream_idx for old version <= 0.30.0
    delete_index("stream_stats_stream_idx", "stream_stats").await?;

    // delete trigger for old version
    // compatible for old version <= 0.6.4
    let client = CLIENT_RW.clone();
    let client = client.lock().await;
    sqlx::query(r#"DROP TRIGGER IF EXISTS update_stream_stats_delete;"#)
        .execute(&*client)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use config::meta::stream::{FileKey, FileMeta, PartitionTimeLevel, StreamStats, StreamType};

    use super::*;
    use crate::file_list::{FileList, FileListJobOrder, FileListJobStatus};

    fn create_test_file_meta() -> FileMeta {
        FileMeta {
            min_ts: 1609459200000000, // 2021-01-01 00:00:00 UTC in microseconds
            max_ts: 1609545600000000, // 2021-01-02 00:00:00 UTC in microseconds
            records: 1000,
            original_size: 50000,
            compressed_size: 10000,
            flattened: false,
            index_size: 5000,
            bloom_ver: 0,
        }
    }

    fn create_test_file_key(account: &str, key: &str, deleted: bool) -> FileKey {
        FileKey {
            account: account.to_string(),
            key: key.to_string(),
            meta: create_test_file_meta(),
            deleted,
            id: 0,
            selection: None,
            row_group_size: None,
            selection_exact: false,
        }
    }

    #[tokio::test]
    async fn test_sqlite_file_list_new() {
        let sqlite_file_list = SqliteFileList::new();
        // zero-sized structs share addresses in release builds — pointer
        // identity is meaningless; constructing it at all is the test
        let _ = &sqlite_file_list;
    }

    #[tokio::test]
    async fn test_sqlite_file_list_default() {
        let default_list = SqliteFileList::default();
        let new_list = SqliteFileList::new();
        assert_eq!(
            std::mem::size_of_val(&default_list),
            std::mem::size_of_val(&new_list)
        );
    }

    #[tokio::test]
    async fn test_parse_file_key_columns_valid_sqlite() {
        let file_key = "files/default/logs/olympics/2021/01/01/00/sqlite_file1.parquet";
        let result = parse_file_key_columns(file_key);

        match result {
            Ok((stream, _date, file)) => {
                assert_eq!(stream, "default/logs/olympics");
                assert_eq!(file, "sqlite_file1.parquet");
            }
            Err(_) => panic!("Should successfully parse valid file key"),
        }
    }

    #[tokio::test]
    async fn test_parse_file_key_columns_invalid_sqlite() {
        let invalid_keys = vec!["", "invalid", "org1/stream1", "org1/stream1/logs"];

        for key in invalid_keys {
            let result = parse_file_key_columns(key);
            assert!(result.is_err(), "Should fail for invalid key: {}", key);
        }
    }

    #[tokio::test]
    async fn test_batch_add_with_id_unimplemented() {
        let sqlite_list = SqliteFileList::new();
        let files = vec![create_test_file_key("account1", "test/key", false)];

        let result = sqlite_list.batch_add_with_id(&files).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_add_empty_files() {
        let sqlite_list = SqliteFileList::new();
        let empty_files: Vec<FileKey> = vec![];

        let result = sqlite_list.batch_add(&empty_files).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_file_key_creation_helpers() {
        let file_key = create_test_file_key(
            "test_account",
            "org/stream/logs/2021/01/01/sqlite_test.parquet",
            false,
        );

        assert_eq!(file_key.account, "test_account");
        assert_eq!(
            file_key.key,
            "org/stream/logs/2021/01/01/sqlite_test.parquet"
        );
        assert!(!file_key.deleted);
        assert_eq!(file_key.id, 0);

        assert_eq!(file_key.meta.records, 1000);
        assert_eq!(file_key.meta.original_size, 50000);
        assert_eq!(file_key.meta.compressed_size, 10000);
        assert!(!file_key.meta.flattened);
    }

    #[tokio::test]
    async fn test_file_meta_creation() {
        let meta = create_test_file_meta();

        assert!(meta.min_ts > 0);
        assert!(meta.max_ts > meta.min_ts);
        assert!(meta.records > 0);
        assert!(meta.original_size > meta.compressed_size);
        assert_eq!(meta.index_size, 5000);
    }

    // Tests for new functionality in fix/file_list_dump branch

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_remove_hard_delete_sqlite() {
        // Test that remove() performs hard delete (DELETE) instead of soft delete
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();
        let file_key = "test_org/logs/test_stream/2021/01/01/00/sqlite_hard_delete_test.parquet";

        // Add a file first
        let _ = sqlite_list.add("test_account", file_key, &meta).await;

        // Verify file exists
        let exists_before = sqlite_list.contains(file_key).await;
        assert!(exists_before.is_ok());

        // Remove the file (should be hard delete now)
        let result = sqlite_list.remove(file_key).await;
        assert!(result.is_ok());

        // Verify file is completely removed (not just marked as deleted)
        let exists_after = sqlite_list.contains(file_key).await;
        assert!(exists_after.is_ok());
        assert!(!exists_after.unwrap());
    }

    /// `add_job` must resurrect a DONE row for the same (stream, hour) and
    /// leave PENDING/RUNNING rows untouched. Regression: an hour whose job
    /// ran while the hour was still OPEN completes without sealing it, and
    /// `ON CONFLICT DO NOTHING` then stranded the closed hour at thousands
    /// of files until the row aged out (prod 2026-07-30).
    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_add_job_resurrects_done_rows_sqlite() {
        let list = SqliteFileList::new();
        let (org, st, stream, offset) = (
            "test_org",
            StreamType::Logs,
            "resurrect_test",
            1785384000000000i64,
        );

        let first = list.add_job(org, st, stream, offset).await.unwrap();
        assert!(first > 0, "first add_job must create the row");

        // a pending row is left alone (same id, still claimable once)
        let again = list.add_job(org, st, stream, offset).await.unwrap();
        assert_eq!(
            again, first,
            "add_job must not duplicate the (stream, hour) row"
        );

        // complete it, as an incremental round does WITHOUT sealing the hour
        raw_set_job(first, FileListJobStatus::Done as i64, "", now_micros()).await;

        // the closed hour is re-queued: the same row returns to Pending
        let resurrected = list.add_job(org, st, stream, offset).await.unwrap();
        assert_eq!(resurrected, first, "resurrection reuses the row");
        let claimed = list
            .get_pending_jobs(
                "test_node",
                10,
                super::super::FileListJobOrder::EnqueueOldest,
                None,
                None,
            )
            .await
            .unwrap();
        assert!(
            claimed.iter().any(|j| j.id == first),
            "a resurrected job must be claimable again"
        );
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_batch_add_with_timestamps_sqlite() {
        // Test that batch_add now includes updated_at timestamps
        let sqlite_list = SqliteFileList::new();
        let files = vec![
            create_test_file_key(
                "account1",
                "org1/logs/stream1/2021/01/01/00/sqlite_file1.parquet",
                false,
            ),
            create_test_file_key(
                "account1",
                "org1/logs/stream1/2021/01/01/00/sqlite_file2.parquet",
                false,
            ),
        ];

        let result = sqlite_list.batch_add(&files).await;
        assert!(result.is_ok());

        // Verify that files were added with timestamps
        for file in &files {
            let exists = sqlite_list.contains(&file.key).await;
            assert!(exists.is_ok());
            assert!(exists.unwrap());
        }
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_query_for_dump_sqlite() {
        // Test the new query_for_dump method
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();

        // Add test files
        let _ = sqlite_list
            .add(
                "test_account",
                "org1/logs/stream1/2021/01/01/00/sqlite_dump_file1.parquet",
                &meta,
            )
            .await;

        // Query for dump with time range
        let time_range = (meta.min_ts - 1000, meta.max_ts + 1000);
        let result = sqlite_list
            .query_for_dump("org1", StreamType::Logs, "stream1", time_range)
            .await;

        // Should return file records
        assert!(result.is_ok());
        let records = result.unwrap();
        assert!(!records.is_empty());
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_query_for_dump_by_updated_at_sqlite() {
        // Test the new query_for_dump_by_updated_at method
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();

        // Add a test file
        let _ = sqlite_list
            .add(
                "test_account",
                "org1/filelist/stream1/2021/01/01/00/sqlite_dump_by_updated.parquet",
                &meta,
            )
            .await;

        // Query by updated_at with a wide time range
        let now = config::utils::time::now_micros();
        let time_range = (now - 60_000_000, now + 60_000_000);
        let result = sqlite_list.query_for_dump_by_updated_at(time_range).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_get_updated_streams_sqlite() {
        // Test the new get_updated_streams method
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();

        // Add test files
        let _ = sqlite_list
            .add(
                "test_account",
                "org1/logs/sqlite_updated_stream1/2021/01/01/00/file1.parquet",
                &meta,
            )
            .await;
        let _ = sqlite_list
            .add(
                "test_account",
                "org1/logs/sqlite_updated_stream2/2021/01/01/00/file2.parquet",
                &meta,
            )
            .await;

        // Query for updated streams
        let now = config::utils::time::now_micros();
        let time_range = (now - 60_000_000, now + 60_000_000);
        let result = sqlite_list.get_updated_streams(time_range).await;

        assert!(result.is_ok());
        let streams = result.unwrap();
        assert!(!streams.is_empty());
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_stats_by_date_range_sqlite() {
        // Test the new stats_by_date_range method
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();

        // Add test files
        let _ = sqlite_list
            .add(
                "test_account",
                "org1/logs/sqlite_stats_stream/2021/01/01/00/stats_file.parquet",
                &meta,
            )
            .await;

        // Query stats by date range
        let date_range = ("2021-01-01".to_string(), "2021-01-02".to_string());
        let result = sqlite_list
            .stats_by_date_range("org1", StreamType::Logs, "sqlite_stats_stream", date_range)
            .await;

        assert!(result.is_ok());
        let stats = result.unwrap();
        assert!(stats.file_num >= 0);
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_set_stream_stats_full_update_sqlite() {
        // Test that set_stream_stats now performs a full update instead of incremental
        let sqlite_list = SqliteFileList::new();

        // Create test stats
        let stats = StreamStats {
            created_at: config::utils::time::now_micros(),
            file_num: 10,
            doc_time_min: 1000000,
            doc_time_max: 2000000,
            doc_num: 1000,
            storage_size: 50000.0,
            compressed_size: 10000.0,
            index_size: 5000.0,
        };

        // Set stream stats (should use INSERT OR REPLACE now)
        let result = sqlite_list
            .set_stream_stats("org1", StreamType::Logs, "sqlite_test_stream", &stats, true)
            .await;

        assert!(result.is_ok());

        // Set different stats for the same stream (should replace, not increment)
        let new_stats = StreamStats {
            created_at: config::utils::time::now_micros(),
            file_num: 5,
            doc_time_min: 1500000,
            doc_time_max: 2500000,
            doc_num: 500,
            storage_size: 25000.0,
            compressed_size: 5000.0,
            index_size: 2500.0,
        };

        let result2 = sqlite_list
            .set_stream_stats(
                "org1",
                StreamType::Logs,
                "sqlite_test_stream",
                &new_stats,
                true,
            )
            .await;

        assert!(result2.is_ok());

        // Verify the stats were replaced, not incremented
        let retrieved_stats = sqlite_list
            .get_stream_stats("org1", Some(StreamType::Logs), Some("sqlite_test_stream"))
            .await;

        assert!(retrieved_stats.is_ok());
        let stats_map = retrieved_stats.unwrap();
        if let Some((_stream_key, retrieved)) = stats_map.first() {
            // The file_num should be 5 (replaced), not 15 (incremented)
            assert_eq!(retrieved.file_num, new_stats.file_num);
        }
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_query_without_deleted_filter_sqlite() {
        // Test that query methods no longer filter by deleted field
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();

        // Add a file
        let file_key = "org1/logs/sqlite_query_stream/2021/01/01/00/query_test.parquet";
        let _ = sqlite_list.add("test_account", file_key, &meta).await;

        // Query files (no longer filters by deleted=false)
        let time_range = (meta.min_ts - 1000, meta.max_ts + 1000);
        let result = sqlite_list
            .query(
                "org1",
                StreamType::Logs,
                "sqlite_query_stream",
                PartitionTimeLevel::Daily,
                time_range,
            )
            .await;

        assert!(result.is_ok());
        let files = result.unwrap();
        assert!(!files.is_empty());
    }

    #[tokio::test]
    #[ignore = "Requires test SQLite database setup"]
    async fn test_query_ids_without_deleted_filter_sqlite() {
        // Test that query_ids no longer filters out deleted records
        let sqlite_list = SqliteFileList::new();
        let meta = create_test_file_meta();

        // Add test files
        let _ = sqlite_list
            .add(
                "test_account",
                "org1/logs/sqlite_ids_stream/2021/01/01/00/ids_file.parquet",
                &meta,
            )
            .await;

        // Query IDs (should not filter by deleted field)
        let time_range = (meta.min_ts - 1000, meta.max_ts + 1000);
        let result = sqlite_list
            .query_ids("org1", StreamType::Logs, "sqlite_ids_stream", time_range)
            .await;

        assert!(result.is_ok());
        let ids = result.unwrap();
        assert!(!ids.is_empty());
    }

    #[tokio::test]
    async fn test_empty_time_range_validation_sqlite() {
        // Test that methods handle time ranges properly
        let sqlite_list = SqliteFileList::new();

        let time_range = (0, 0);

        // query_ids should process the query instead of returning early
        let result = sqlite_list
            .query_ids("org1", StreamType::Logs, "test", time_range)
            .await;

        // Should attempt the query (may fail due to no database, but shouldn't short-circuit)
        let _ = result;
    }

    // ── bloom_ver schema migration + round-trip on a fresh in-memory DB ──────
    //
    // These tests exercise the column add path and the FromRow mapping without
    // touching the global CLIENT_RW.

    async fn fresh_in_memory_pool() -> sqlx::Pool<sqlx::Sqlite> {
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite")
    }

    /// Old-shape file_list table that pre-dates the bloom_ver column.
    /// Mirrors the historical DDL plus all columns the migration block adds in order.
    async fn create_legacy_file_list_table(pool: &sqlx::Pool<sqlx::Sqlite>) {
        sqlx::query(
            r#"
            CREATE TABLE file_list (
                id              INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                account         VARCHAR DEFAULT '' NOT NULL,
                org             VARCHAR NOT NULL,
                stream          VARCHAR NOT NULL,
                date            VARCHAR NOT NULL,
                file            VARCHAR NOT NULL,
                deleted         BOOLEAN DEFAULT false NOT NULL,
                flattened       BOOLEAN DEFAULT false NOT NULL,
                min_ts          BIGINT NOT NULL,
                max_ts          BIGINT NOT NULL,
                records         BIGINT NOT NULL,
                original_size   BIGINT NOT NULL,
                compressed_size BIGINT NOT NULL,
                index_size      BIGINT DEFAULT 0 NOT NULL,
                updated_at      BIGINT DEFAULT 0 NOT NULL
            );
            "#,
        )
        .execute(pool)
        .await
        .expect("legacy table");
    }

    async fn column_exists(pool: &sqlx::Pool<sqlx::Sqlite>, table: &str, col: &str) -> bool {
        let rows: Vec<(i64, String, String, i64, Option<String>, i64)> =
            sqlx::query_as(&format!("PRAGMA table_info({table});"))
                .fetch_all(pool)
                .await
                .unwrap();
        rows.iter().any(|(_, name, ..)| name == col)
    }

    #[tokio::test]
    async fn test_add_column_bloom_ver_migration_idempotent() {
        let pool = fresh_in_memory_pool().await;
        create_legacy_file_list_table(&pool).await;

        // Pre-state: bloom_ver does not exist.
        assert!(!column_exists(&pool, "file_list", "bloom_ver").await);

        // Apply the migration once — should add the column.
        crate::db::sqlite::add_column(&pool, "file_list", "bloom_ver", "BIGINT default 0 not null")
            .await
            .expect("first add_column");
        assert!(column_exists(&pool, "file_list", "bloom_ver").await);

        // Apply again — must be idempotent (no error, no duplicate column).
        crate::db::sqlite::add_column(&pool, "file_list", "bloom_ver", "BIGINT default 0 not null")
            .await
            .expect("second add_column should be idempotent");
        assert!(column_exists(&pool, "file_list", "bloom_ver").await);
    }

    #[tokio::test]
    async fn test_legacy_rows_default_to_bloom_ver_zero_after_migration() {
        let pool = fresh_in_memory_pool().await;
        create_legacy_file_list_table(&pool).await;

        // Insert a row in the legacy schema (no bloom_ver column).
        sqlx::query(
            r#"INSERT INTO file_list
                (account, org, stream, date, file, deleted, flattened,
                 min_ts, max_ts, records, original_size, compressed_size,
                 index_size, updated_at)
               VALUES ('a', 'o', 's/logs/x', '2026/05/08/00', 'legacy.parquet',
                       false, false, 1, 2, 3, 4, 5, 6, 7);"#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Migrate.
        crate::db::sqlite::add_column(&pool, "file_list", "bloom_ver", "BIGINT default 0 not null")
            .await
            .unwrap();

        // FromRow reads — legacy row should expose bloom_ver = 0.
        let rec: FileRecord = sqlx::query_as(
            r#"SELECT id, account, stream, date, file, deleted, flattened,
                       min_ts, max_ts, records, original_size, compressed_size,
                       index_size, bloom_ver, updated_at
               FROM file_list WHERE file = 'legacy.parquet';"#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rec.bloom_ver, 0);
    }

    #[tokio::test]
    async fn test_bloom_ver_round_trip_through_sqlite() {
        // End-to-end: write a row with non-zero bloom_ver, read it back, confirm value preserved.
        let pool = fresh_in_memory_pool().await;
        create_legacy_file_list_table(&pool).await;
        crate::db::sqlite::add_column(&pool, "file_list", "bloom_ver", "BIGINT default 0 not null")
            .await
            .unwrap();

        let bv: i64 = 1_715_000_000_000_000;
        sqlx::query(
            r#"INSERT INTO file_list
                (account, org, stream, date, file, deleted, flattened,
                 min_ts, max_ts, records, original_size, compressed_size,
                 index_size, bloom_ver, updated_at)
               VALUES ('a', 'o', 's/logs/x', '2026/05/08/00', 'with_bv.parquet',
                       false, false, 1, 2, 3, 4, 5, 6, ?, 7);"#,
        )
        .bind(bv)
        .execute(&pool)
        .await
        .unwrap();

        let rec: FileRecord = sqlx::query_as(
            r#"SELECT id, account, stream, date, file, deleted, flattened,
                       min_ts, max_ts, records, original_size, compressed_size,
                       index_size, bloom_ver, updated_at
               FROM file_list WHERE file = 'with_bv.parquet';"#,
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(rec.bloom_ver, bv);

        // Conversion to FileMeta must carry the value through.
        let meta = FileMeta::from(&rec);
        assert_eq!(meta.bloom_ver, bv);
    }

    /// The bloom pending queue matches `bloom_ver = 0` STRICTLY: the
    /// UNBUILDABLE stamp (-2) — like NO_BLOOM (-1) — takes a poison file out
    /// of the queue after one attempt, while the pruner (`bloom_ver <= 0`)
    /// keeps the file un-pruned so queries stay correct.
    #[tokio::test]
    async fn test_bloom_pending_queue_excludes_unbuildable_stamp() {
        let pool = fresh_in_memory_pool().await;
        create_legacy_file_list_table(&pool).await;
        crate::db::sqlite::add_column(&pool, "file_list", "bloom_ver", "BIGINT default 0 not null")
            .await
            .unwrap();

        for (file, bloom_ver) in [
            ("pending.parquet", 0i64),
            ("no_bloom.parquet", -1),
            ("unbuildable.parquet", -2),
            ("built.parquet", 1_715_000_000_000_000),
        ] {
            sqlx::query(
                r#"INSERT INTO file_list
                    (account, org, stream, date, file, deleted, flattened,
                     min_ts, max_ts, records, original_size, compressed_size,
                     index_size, bloom_ver, updated_at)
                   VALUES ('a', 'o', 'o/logs/s', '2026/08/01/00', ?, false, false,
                           1, 2, 3, 4, 5, 6, ?, 7);"#,
            )
            .bind(file)
            .bind(bloom_ver)
            .execute(&pool)
            .await
            .unwrap();
        }

        // the exact predicate query_for_bloom / query_bloom_pending_buckets use
        let pending: Vec<(String,)> = sqlx::query_as(
            r#"SELECT file FROM file_list
               WHERE stream = 'o/logs/s' AND date = '2026/08/01/00'
                 AND index_size > 0 AND bloom_ver = 0;"#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            pending,
            vec![("pending.parquet".to_string(),)],
            "only bloom_ver = 0 stays in the queue; -2 is retried never, not forever"
        );

        // and the pruner-side predicate still keeps the poison file visible
        let unpruned: Vec<(String,)> = sqlx::query_as(
            r#"SELECT file FROM file_list
               WHERE stream = 'o/logs/s' AND bloom_ver <= 0 ORDER BY file;"#,
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(unpruned.len(), 3, "0, -1 and -2 all stay un-pruned");
    }

    // ── delete_by_org row move ───────────────────────────────────────────────
    //
    // `file_list` has no `index_file` column; the move statement writes a
    // constant false (no file has a sibling index object). Regression test:
    // an earlier revision selected a non-existent column and blew up at
    // runtime, breaking org cleanup for every file type — keep the statement
    // pinned to the real schema.

    #[tokio::test]
    async fn test_move_file_list_to_deleted_writes_index_file_false() {
        let pool = fresh_in_memory_pool().await;
        // current-shape tables (subset of create_table used by the statement)
        sqlx::query(
            r#"
            CREATE TABLE file_list (
                id              INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                account         VARCHAR DEFAULT '' NOT NULL,
                org             VARCHAR NOT NULL,
                stream          VARCHAR NOT NULL,
                date            VARCHAR NOT NULL,
                file            VARCHAR NOT NULL,
                deleted         BOOLEAN DEFAULT false NOT NULL,
                flattened       BOOLEAN DEFAULT false NOT NULL,
                min_ts          BIGINT NOT NULL,
                max_ts          BIGINT NOT NULL,
                records         BIGINT NOT NULL,
                original_size   BIGINT NOT NULL,
                compressed_size BIGINT NOT NULL,
                index_size      BIGINT DEFAULT 0 NOT NULL,
                bloom_ver       BIGINT DEFAULT 0 NOT NULL,
                updated_at      BIGINT DEFAULT 0 NOT NULL
            );
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            r#"
            CREATE TABLE file_list_deleted (
                id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                account    VARCHAR NOT NULL,
                org        VARCHAR NOT NULL,
                stream     VARCHAR NOT NULL,
                date       VARCHAR NOT NULL,
                file       VARCHAR NOT NULL,
                index_file BOOLEAN DEFAULT false NOT NULL,
                flattened  BOOLEAN DEFAULT false NOT NULL,
                created_at BIGINT NOT NULL
            );
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // a core .vix (index embedded, index_size > 0) and a parquet
        for (file, index_size) in [("1.vix", 100_i64), ("2.parquet", 0)] {
            sqlx::query(
                r#"INSERT INTO file_list (account, org, stream, date, file, min_ts, max_ts, records, original_size, compressed_size, index_size)
                   VALUES ('acc', 'org1', 'org1/logs/s1', '2024/02/16/16', $1, 1, 2, 10, 1000, 100, $2);"#,
            )
            .bind(file)
            .bind(index_size)
            .execute(&pool)
            .await
            .unwrap();
        }

        sqlx::query(super::super::MOVE_FILE_LIST_TO_DELETED_SQL)
            .bind("org1")
            .bind(123_456_789_i64)
            .execute(&pool)
            .await
            .expect("move statement must be valid against the file_list schema");

        let rows: Vec<(String, bool, i64)> = sqlx::query_as(
            "SELECT file, index_file, created_at FROM file_list_deleted ORDER BY file;",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                ("1.vix".to_string(), false, 123_456_789),
                ("2.parquet".to_string(), false, 123_456_789),
            ],
            "index_file is a constant false for every file"
        );
    }

    // ── generation-fenced merge and dump leases ─────────────────────────────
    //
    // Runs against the process-global sqlite pools (same setup as the
    // `infra::wal_segments` tests): serialize on a module lock, create the
    // real tables once, and namespace rows by a per-run unique stream so
    // other modules sharing the sqlite file are never touched.

    static JOBS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    const LEGACY_ADD_JOB_SQL: &str = r#"INSERT INTO file_list_jobs
    (org, stream, offsets, status, node, started_at, updated_at)
VALUES ($1, $2, $3, $4, '', 0, 0)
ON CONFLICT (stream, offsets) DO UPDATE SET
    status = $4,
    node = '',
    started_at = 0
WHERE file_list_jobs.status = $5;"#;

    async fn jobs_setup() -> tokio::sync::MutexGuard<'static, ()> {
        let guard = JOBS_TEST_LOCK.lock().await;
        std::fs::create_dir_all(&get_config().common.data_db_dir)
            .expect("create data_db_dir for tests");
        create_table().await.expect("create file_list tables");
        // the unique (stream, offsets) index is what add_job's ON CONFLICT
        // clause resolves against
        create_table_index()
            .await
            .expect("create file_list indexes");
        guard
    }

    async fn raw_set_job(id: i64, status: i64, node: &str, updated_at: i64) {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query(
            "UPDATE file_list_jobs SET status = $1, node = $2, updated_at = $3 WHERE id = $4;",
        )
        .bind(status)
        .bind(node)
        .bind(updated_at)
        .bind(id)
        .execute(&*client)
        .await
        .unwrap_or_else(|e| panic!("raw set job {id} failed: {e}"));
    }

    async fn raw_job_lease_row(id: i64) -> (i64, String, bool, i64) {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query_as::<_, (i64, String, bool, i64)>(
            "SELECT status, node, dumped, lease_generation FROM file_list_jobs WHERE id = $1;",
        )
        .bind(id)
        .fetch_one(&*client)
        .await
        .unwrap_or_else(|e| panic!("raw job lease row {id} failed: {e}"))
    }

    async fn raw_job_pending_after_dump(id: i64) -> bool {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query_scalar("SELECT pending_after_dump FROM file_list_jobs WHERE id = $1;")
            .bind(id)
            .fetch_one(&*client)
            .await
            .unwrap_or_else(|e| panic!("raw pending-after-dump row {id} failed: {e}"))
    }

    async fn raw_job_merge_row(id: i64) -> (i64, String, i64, i64, bool, bool) {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query_as(
            "SELECT status, node, started_at, lease_generation, \
             pending_after_run, pending_after_dump \
             FROM file_list_jobs WHERE id = $1;",
        )
        .bind(id)
        .fetch_one(&*client)
        .await
        .unwrap_or_else(|e| panic!("raw merge job row {id} failed: {e}"))
    }

    async fn raw_delete_jobs(ids: &[i64]) {
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        for id in ids {
            sqlx::query("DELETE FROM file_list_jobs WHERE id = $1;")
                .bind(id)
                .execute(&*client)
                .await
                .unwrap_or_else(|e| panic!("raw delete job {id} failed: {e}"));
        }
    }

    #[tokio::test]
    async fn test_generation_fences_same_node_aba_and_stale_reset_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let stream = format!("generation_aba_{offset}");
        let id = list
            .add_job("fence_org", StreamType::Logs, &stream, offset)
            .await
            .unwrap();

        let first_claim = list
            .get_pending_jobs(
                "same-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap();
        let first = first_claim.iter().find(|job| job.id == id).unwrap();
        let first_generation = first.lease_generation;
        assert!(
            list.touch_job_lease(
                id,
                "same-node",
                first_generation,
                FileListJobStatus::Running,
            )
            .await
            .unwrap()
        );
        assert!(
            list.set_job_pending_owned(id, "same-node", first_generation)
                .await
                .unwrap()
        );

        let second_claim = list
            .get_pending_jobs(
                "same-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap();
        let second = second_claim.iter().find(|job| job.id == id).unwrap();
        let second_generation = second.lease_generation;
        assert_eq!(second_generation, first_generation + 1);
        assert!(
            !list
                .touch_job_lease(
                    id,
                    "same-node",
                    first_generation,
                    FileListJobStatus::Running,
                )
                .await
                .unwrap()
        );
        assert!(
            !list
                .set_job_pending_owned(id, "same-node", first_generation)
                .await
                .unwrap()
        );
        assert!(
            !list
                .set_job_done_owned(id, "same-node", first_generation)
                .await
                .unwrap()
        );

        const STALE_AT: i64 = 1_700_000_000_000_000;
        raw_set_job(id, 1, "same-node", STALE_AT).await;
        list.check_running_jobs(STALE_AT + 1).await.unwrap();
        let (status, node, _, reset_generation) = raw_job_lease_row(id).await;
        assert_eq!(status, FileListJobStatus::Pending as i64);
        assert!(node.is_empty());
        assert_eq!(reset_generation, second_generation + 1);
        assert!(
            !list
                .touch_job_lease(
                    id,
                    "same-node",
                    second_generation,
                    FileListJobStatus::Running,
                )
                .await
                .unwrap()
        );
        assert!(
            !list
                .set_job_done_owned(id, "same-node", second_generation)
                .await
                .unwrap()
        );
        let stream_key = format!("fence_org/logs/{stream}");
        assert_eq!(
            list.reset_jobs_admin(offset, Some(&stream_key))
                .await
                .unwrap(),
            1
        );
        let (status, node, _, admin_generation) = raw_job_lease_row(id).await;
        assert_eq!(status, FileListJobStatus::Pending as i64);
        assert!(node.is_empty());
        assert_eq!(admin_generation, reset_generation + 1);
        raw_delete_jobs(&[id]).await;
    }

    /// A requeued stable-id job must move behind older waiting work.
    /// Otherwise an old hour that remains a deterministic no-op is claimed
    /// every sweep and newer mergeable hours never run.
    #[tokio::test]
    async fn test_enqueue_oldest_uses_requeue_time_before_stable_id_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let base = now_micros();
        let old_id = list
            .add_job(
                "fair_queue_org",
                StreamType::Logs,
                &format!("fair_queue_old_{base}"),
                base,
            )
            .await
            .unwrap();
        let waiting_id = list
            .add_job(
                "fair_queue_org",
                StreamType::Logs,
                &format!("fair_queue_waiting_{base}"),
                base + 1,
            )
            .await
            .unwrap();
        assert!(old_id < waiting_id, "test requires stable-id order");

        // Give only the old row one turn, then requeue it as a completed
        // debt sweep would. This stamps its enqueue clock after the still-
        // waiting row while retaining the smaller stable id.
        let first = list
            .get_pending_jobs(
                "fair-queue-node",
                1,
                FileListJobOrder::EnqueueOldest,
                Some(base),
                Some(base + 2),
            )
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].id, old_id);
        assert!(
            list.set_job_pending_owned(first[0].id, "fair-queue-node", first[0].lease_generation,)
                .await
                .unwrap()
        );

        let claimed = list
            .get_pending_jobs(
                "fair-queue-node",
                1,
                FileListJobOrder::EnqueueOldest,
                Some(base),
                Some(base + 2),
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(
            claimed[0].id, waiting_id,
            "older waiting work must run before a requeued low-id row"
        );
        raw_delete_jobs(&[old_id, waiting_id]).await;
    }

    #[tokio::test]
    async fn test_offset_newest_rotates_requeued_row_within_same_hour_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let first_id = list
            .add_job(
                "fast_fair_org",
                StreamType::Logs,
                &format!("fast_fair_first_{offset}"),
                offset,
            )
            .await
            .unwrap();
        let waiting_id = list
            .add_job(
                "fast_fair_org",
                StreamType::Logs,
                &format!("fast_fair_waiting_{offset}"),
                offset,
            )
            .await
            .unwrap();
        assert!(first_id < waiting_id, "test requires stable-id order");

        let first = list
            .get_pending_jobs(
                "fast-fair-node",
                1,
                FileListJobOrder::OffsetNewest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].id, first_id);
        assert!(
            list.set_job_pending_owned(first_id, "fast-fair-node", first[0].lease_generation)
                .await
                .unwrap()
        );

        let next = list
            .get_pending_jobs(
                "fast-fair-node",
                1,
                FileListJobOrder::OffsetNewest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(
            next[0].id, waiting_id,
            "same-hour work with the older enqueue clock must precede the requeued row"
        );
        raw_delete_jobs(&[first_id, waiting_id]).await;
    }

    #[tokio::test]
    async fn test_claim_windows_order_and_dump_done_lease_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let base = now_micros();
        let mut ids = Vec::new();
        for delta in 0..=3_i64 {
            ids.push(
                list.add_job(
                    "window_org",
                    StreamType::Logs,
                    &format!("window_{base}_{delta}"),
                    base + delta,
                )
                .await
                .unwrap(),
            );
        }

        let newest = list
            .get_pending_jobs(
                "window-node",
                100,
                FileListJobOrder::OffsetNewest,
                Some(base),
                Some(base + 3),
            )
            .await
            .unwrap();
        let ours = newest
            .iter()
            .filter(|job| ids.contains(&job.id))
            .collect::<Vec<_>>();
        assert_eq!(
            ours.iter().map(|job| job.offsets).collect::<Vec<_>>(),
            vec![base + 2, base + 1, base]
        );
        assert!(!ours.iter().any(|job| job.offsets == base + 3));
        for job in ours {
            assert!(
                list.set_job_pending_owned(job.id, "window-node", job.lease_generation)
                    .await
                    .unwrap()
            );
        }

        let oldest = list
            .get_pending_jobs(
                "window-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(base + 1),
                Some(base + 3),
            )
            .await
            .unwrap();
        let ours = oldest
            .iter()
            .filter(|job| ids.contains(&job.id))
            .collect::<Vec<_>>();
        assert_eq!(
            ours.iter().map(|job| job.id).collect::<Vec<_>>(),
            vec![ids[1], ids[2]]
        );
        for job in ours {
            list.set_job_done_owned(job.id, "window-node", job.lease_generation)
                .await
                .unwrap();
        }

        let dump_id = ids[1];
        {
            let client = CLIENT_RW.clone();
            let client = client.lock().await;
            sqlx::query(
                "UPDATE file_list_jobs SET status = $1, dumped = false, node = '', updated_at = 0 \
                 WHERE id = $2;",
            )
            .bind(FileListJobStatus::Done)
            .bind(dump_id)
            .execute(&*client)
            .await
            .unwrap();
        }
        let before_dump_generation = raw_job_lease_row(dump_id).await.3;
        let dump_claims = list
            .get_pending_dump_jobs("dump-node", 10_000)
            .await
            .unwrap();
        let stranger_dump_leases = dump_claims
            .iter()
            .filter(|job| job.id != dump_id)
            .map(|job| (job.id, job.lease_generation))
            .collect::<Vec<_>>();
        for (id, generation) in stranger_dump_leases {
            list.set_job_dumped_status_owned(id, "dump-node", generation, false)
                .await
                .unwrap();
        }
        let dump = dump_claims.iter().find(|job| job.id == dump_id).unwrap();
        let dump_generation = dump.lease_generation;
        assert_eq!(dump_generation, before_dump_generation + 1);
        assert!(
            list.touch_job_lease(
                dump_id,
                "dump-node",
                dump_generation,
                FileListJobStatus::Done,
            )
            .await
            .unwrap()
        );
        assert!(
            !list
                .touch_job_lease(
                    dump_id,
                    "dump-node",
                    dump_generation,
                    FileListJobStatus::Running,
                )
                .await
                .unwrap()
        );

        // A late add while the dump lease is owned records a durable requeue.
        let same_id = list
            .add_job(
                "window_org",
                StreamType::Logs,
                &format!("window_{base}_1"),
                base + 1,
            )
            .await
            .unwrap();
        assert_eq!(same_id, dump_id);
        let (status, node, dumped, generation) = raw_job_lease_row(dump_id).await;
        assert_eq!(status, FileListJobStatus::Done as i64);
        assert_eq!(node, "dump-node");
        assert!(!dumped);
        assert_eq!(generation, dump_generation);
        assert!(raw_job_pending_after_dump(dump_id).await);
        assert!(!raw_job_merge_row(dump_id).await.4);

        // Failure ends the lease but preserves the marker for a successful retry.
        assert!(
            list.set_job_dumped_status_owned(dump_id, "dump-node", dump_generation, false)
                .await
                .unwrap()
        );
        let (status, node, dumped, generation) = raw_job_lease_row(dump_id).await;
        assert_eq!(status, FileListJobStatus::Done as i64);
        assert!(node.is_empty());
        assert!(!dumped);
        assert_eq!(generation, dump_generation);
        assert!(raw_job_pending_after_dump(dump_id).await);
        assert!(!raw_job_merge_row(dump_id).await.4);

        let retry_claims = list
            .get_pending_dump_jobs("dump-node", 10_000)
            .await
            .unwrap();
        let retry = retry_claims.iter().find(|job| job.id == dump_id).unwrap();
        let retry_generation = retry.lease_generation;
        assert_eq!(retry_generation, dump_generation + 1);
        let stranger_dump_leases = retry_claims
            .iter()
            .filter(|job| job.id != dump_id)
            .map(|job| (job.id, job.lease_generation))
            .collect::<Vec<_>>();
        for (id, generation) in stranger_dump_leases {
            list.set_job_dumped_status_owned(id, "dump-node", generation, false)
                .await
                .unwrap();
        }

        // Timing out the retry increments the fence without losing the marker.
        const STALE_DUMP_AT: i64 = 1_700_000_000_000_000;
        raw_set_job(
            dump_id,
            FileListJobStatus::Done as i64,
            "dump-node",
            STALE_DUMP_AT,
        )
        .await;
        list.check_running_jobs(STALE_DUMP_AT + 1).await.unwrap();
        let (_, node, _, reset_generation) = raw_job_lease_row(dump_id).await;
        assert!(node.is_empty());
        assert_eq!(reset_generation, retry_generation + 1);
        assert!(raw_job_pending_after_dump(dump_id).await);
        assert!(!raw_job_merge_row(dump_id).await.4);
        assert!(
            !list
                .set_job_dumped_status_owned(dump_id, "dump-node", retry_generation, true)
                .await
                .unwrap()
        );

        let reclaimed = list
            .get_pending_dump_jobs("dump-node", 10_000)
            .await
            .unwrap();
        let stranger_dump_leases = reclaimed
            .iter()
            .filter(|job| job.id != dump_id)
            .map(|job| (job.id, job.lease_generation))
            .collect::<Vec<_>>();
        for (id, generation) in stranger_dump_leases {
            list.set_job_dumped_status_owned(id, "dump-node", generation, false)
                .await
                .unwrap();
        }
        let reclaimed = reclaimed.iter().find(|job| job.id == dump_id).unwrap();
        assert_eq!(reclaimed.lease_generation, reset_generation + 1);
        assert!(
            list.set_job_dumped_status_owned(
                dump_id,
                "dump-node",
                reclaimed.lease_generation,
                true,
            )
            .await
            .unwrap()
        );
        let (status, node, dumped, _) = raw_job_lease_row(dump_id).await;
        assert_eq!(status, FileListJobStatus::Pending as i64);
        assert!(node.is_empty());
        assert!(!dumped);
        assert!(!raw_job_pending_after_dump(dump_id).await);
        assert!(!raw_job_merge_row(dump_id).await.4);
        raw_delete_jobs(&ids).await;
    }

    #[tokio::test]
    async fn test_running_retrigger_is_durable_and_fenced_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let stream = format!("running_retrigger_{offset}");
        let id = list
            .add_job("retrigger_org", StreamType::Logs, &stream, offset)
            .await
            .unwrap();

        let first = list
            .get_pending_jobs(
                "merge-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        let before = raw_job_merge_row(id).await;
        assert_eq!(before.0, FileListJobStatus::Running as i64);
        assert_eq!(before.1, "merge-node");
        assert!(!before.4);

        assert_eq!(
            list.add_job("retrigger_org", StreamType::Logs, &stream, offset)
                .await
                .unwrap(),
            id
        );
        let latched = raw_job_merge_row(id).await;
        assert_eq!(latched.0, FileListJobStatus::Running as i64);
        assert_eq!(latched.1, "merge-node");
        assert_eq!(latched.3, first.lease_generation);
        assert!(latched.4);
        assert!(!latched.5, "merge and dump latches are independent");

        assert!(
            !list
                .set_job_done_owned(id, "merge-node", first.lease_generation - 1)
                .await
                .unwrap()
        );
        assert!(
            raw_job_merge_row(id).await.4,
            "a stale owner must not consume the retrigger"
        );
        assert!(
            list.set_job_done_owned(id, "merge-node", first.lease_generation)
                .await
                .unwrap()
        );
        let requeued = raw_job_merge_row(id).await;
        assert_eq!(requeued.0, FileListJobStatus::Pending as i64);
        assert!(requeued.1.is_empty());
        assert_eq!(requeued.2, 0);
        assert!(!requeued.4);
        assert!(!requeued.5);

        let second = list
            .get_pending_jobs(
                "merge-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        assert_eq!(second.lease_generation, first.lease_generation + 1);
        assert!(!raw_job_merge_row(id).await.4);
        assert!(
            list.set_job_done_owned(id, "merge-node", second.lease_generation)
                .await
                .unwrap()
        );
        assert_eq!(
            raw_job_merge_row(id).await.0,
            FileListJobStatus::Done as i64
        );
        assert!(
            !list
                .set_job_done_owned(id, "merge-node", second.lease_generation)
                .await
                .unwrap(),
            "a normal completion is accepted exactly once"
        );
        raw_delete_jobs(&[id]).await;
    }

    #[tokio::test]
    async fn test_legacy_add_and_completion_honor_running_latch_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let stream = format!("legacy_retrigger_{offset}");
        let stream_key = format!("retrigger_org/logs/{stream}");

        // The trigger must not interfere with the ordinary insert arm of the
        // exact legacy producer statement.
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let inserted = sqlx::query(LEGACY_ADD_JOB_SQL)
            .bind("retrigger_org")
            .bind(&stream_key)
            .bind(offset)
            .bind(FileListJobStatus::Pending)
            .bind(FileListJobStatus::Done)
            .execute(&*client)
            .await
            .unwrap();
        assert_eq!(inserted.rows_affected(), 1);
        let id: i64 =
            sqlx::query_scalar("SELECT id FROM file_list_jobs WHERE stream = $1 AND offsets = $2;")
                .bind(&stream_key)
                .bind(offset)
                .fetch_one(&*client)
                .await
                .unwrap();
        drop(client);

        let claim = list
            .get_pending_jobs(
                "old-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();

        // The legacy conflict update is gated to DONE. The BEFORE INSERT
        // trigger must instead latch the request while preserving the active
        // lease. Repeated arrivals coalesce into the same durable rerun.
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        for _ in 0..2 {
            let ignored = sqlx::query(LEGACY_ADD_JOB_SQL)
                .bind("retrigger_org")
                .bind(&stream_key)
                .bind(offset)
                .bind(FileListJobStatus::Pending)
                .bind(FileListJobStatus::Done)
                .execute(&*client)
                .await
                .unwrap();
            assert_eq!(ignored.rows_affected(), 0);
        }
        drop(client);

        let latched = raw_job_merge_row(id).await;
        assert_eq!(latched.0, FileListJobStatus::Running as i64);
        assert_eq!(latched.1, "old-node");
        assert_eq!(latched.3, claim.lease_generation);
        assert!(latched.4);

        // Exact legacy workers completed a batch by id without an ownership
        // predicate or pending_after_run column. The status transition trigger
        // consumes the latch and converts that completion into one rerun.
        let legacy_completion_sql = format!(
            "UPDATE file_list_jobs SET status = $1, updated_at = $2, dumped = $3, node = '' \
             WHERE id IN ({id});"
        );
        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let completed = sqlx::query(&legacy_completion_sql)
            .bind(FileListJobStatus::Done)
            .bind(now_micros())
            .bind(!get_config().compact.file_list_dump_enabled)
            .execute(&*client)
            .await
            .unwrap();
        assert_eq!(completed.rows_affected(), 1);
        drop(client);

        let requeued = raw_job_merge_row(id).await;
        assert_eq!(requeued.0, FileListJobStatus::Pending as i64);
        assert!(requeued.1.is_empty());
        assert_eq!(requeued.2, 0);
        assert!(!requeued.4);

        let rerun = list
            .get_pending_jobs(
                "old-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        assert_eq!(rerun.lease_generation, claim.lease_generation + 1);
        assert!(!raw_job_merge_row(id).await.4);

        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let rerun_completed = sqlx::query(&legacy_completion_sql)
            .bind(FileListJobStatus::Done)
            .bind(now_micros())
            .bind(!get_config().compact.file_list_dump_enabled)
            .execute(&*client)
            .await
            .unwrap();
        assert_eq!(rerun_completed.rows_affected(), 1);
        drop(client);
        assert_eq!(
            raw_job_merge_row(id).await.0,
            FileListJobStatus::Done as i64
        );
        raw_delete_jobs(&[id]).await;
    }

    #[tokio::test]
    async fn test_legacy_add_and_dump_completion_preserve_owned_dump_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let stream = format!("legacy_dump_retrigger_{offset}");
        let stream_key = format!("retrigger_org/logs/{stream}");
        let id = list
            .add_job("retrigger_org", StreamType::Logs, &stream, offset)
            .await
            .unwrap();

        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        sqlx::query(
            "UPDATE file_list_jobs \
             SET status = $1, node = 'legacy-dump-node', started_at = 42, \
                 updated_at = $2, dumped = false, lease_generation = lease_generation + 1 \
             WHERE id = $3;",
        )
        .bind(FileListJobStatus::Done)
        .bind(now_micros())
        .bind(id)
        .execute(&*client)
        .await
        .unwrap();
        for _ in 0..2 {
            let ignored = sqlx::query(LEGACY_ADD_JOB_SQL)
                .bind("retrigger_org")
                .bind(&stream_key)
                .bind(offset)
                .bind(FileListJobStatus::Pending)
                .bind(FileListJobStatus::Done)
                .execute(&*client)
                .await
                .unwrap();
            assert_eq!(ignored.rows_affected(), 0);
        }
        drop(client);

        let owned = raw_job_merge_row(id).await;
        assert_eq!(owned.0, FileListJobStatus::Done as i64);
        assert_eq!(owned.1, "legacy-dump-node");
        assert_eq!(owned.2, 42);
        assert_eq!(owned.3, 1);
        assert!(!owned.4);
        assert!(owned.5);

        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let completed = sqlx::query(
            "UPDATE file_list_jobs SET dumped = true, node = '', updated_at = $1 WHERE id = $2;",
        )
        .bind(now_micros())
        .bind(id)
        .execute(&*client)
        .await
        .unwrap();
        assert_eq!(completed.rows_affected(), 1);
        drop(client);

        let requeued = raw_job_merge_row(id).await;
        assert_eq!(requeued.0, FileListJobStatus::Pending as i64);
        assert!(requeued.1.is_empty());
        assert_eq!(requeued.2, 0);
        assert!(!requeued.4);
        assert!(!requeued.5);
        assert!(!raw_job_lease_row(id).await.2);

        let rerun = list
            .get_pending_jobs(
                "merge-after-dump",
                1,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap();
        assert_eq!(rerun.len(), 1);
        assert_eq!(rerun[0].id, id);
        assert!(
            list.set_job_done_owned(id, "merge-after-dump", rerun[0].lease_generation)
                .await
                .unwrap()
        );
        assert!(
            list.get_pending_jobs(
                "no-second-rerun",
                1,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .is_empty()
        );
        raw_delete_jobs(&[id]).await;
    }

    #[tokio::test]
    async fn test_stale_legacy_completion_cannot_consume_pending_rerun_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let stream = format!("legacy_stale_completion_{offset}");
        let stream_key = format!("retrigger_org/traces/{stream}");
        let id = list
            .add_job("retrigger_org", StreamType::Traces, &stream, offset)
            .await
            .unwrap();
        let first = list
            .get_pending_jobs(
                "timed-out-old-node",
                1,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .pop()
            .unwrap();

        let client = CLIENT_RW.clone();
        let client = client.lock().await;
        let ignored = sqlx::query(LEGACY_ADD_JOB_SQL)
            .bind("retrigger_org")
            .bind(&stream_key)
            .bind(offset)
            .bind(FileListJobStatus::Pending)
            .bind(FileListJobStatus::Done)
            .execute(&*client)
            .await
            .unwrap();
        assert_eq!(ignored.rows_affected(), 0);
        sqlx::query(
            "UPDATE file_list_jobs \
             SET status = $1, node = '', lease_generation = lease_generation + 1 \
             WHERE id = $2;",
        )
        .bind(FileListJobStatus::Pending)
        .bind(id)
        .execute(&*client)
        .await
        .unwrap();
        let stale_completed = sqlx::query(
            "UPDATE file_list_jobs \
             SET status = $1, updated_at = $2, dumped = true, node = '' \
             WHERE id = $3;",
        )
        .bind(FileListJobStatus::Done)
        .bind(now_micros())
        .bind(id)
        .execute(&*client)
        .await
        .unwrap();
        assert_eq!(stale_completed.rows_affected(), 1);
        drop(client);

        let preserved = raw_job_merge_row(id).await;
        assert_eq!(preserved.0, FileListJobStatus::Pending as i64);
        assert!(preserved.1.is_empty());
        assert_eq!(preserved.2, 0);
        assert!(!preserved.4);
        assert!(!raw_job_lease_row(id).await.2);

        let rerun = list
            .get_pending_jobs(
                "replacement-node",
                1,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap();
        assert_eq!(rerun.len(), 1);
        assert_eq!(rerun[0].id, id);
        assert_eq!(rerun[0].lease_generation, first.lease_generation + 2);
        raw_delete_jobs(&[id]).await;
    }

    #[tokio::test]
    async fn test_failed_merge_retry_subsumes_running_latch_sqlite() {
        let _guard = jobs_setup().await;
        let list = SqliteFileList::new();
        let offset = now_micros();
        let stream = format!("failed_retrigger_{offset}");
        let id = list
            .add_job("retrigger_org", StreamType::Traces, &stream, offset)
            .await
            .unwrap();
        let first = list
            .get_pending_jobs(
                "failure-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();

        list.add_job("retrigger_org", StreamType::Traces, &stream, offset)
            .await
            .unwrap();
        assert!(raw_job_merge_row(id).await.4);
        assert!(
            list.set_job_pending_owned(id, "failure-node", first.lease_generation)
                .await
                .unwrap()
        );
        assert!(raw_job_merge_row(id).await.4);

        let retry = list
            .get_pending_jobs(
                "failure-node",
                100,
                FileListJobOrder::EnqueueOldest,
                Some(offset),
                Some(offset + 1),
            )
            .await
            .unwrap()
            .into_iter()
            .find(|job| job.id == id)
            .unwrap();
        assert!(!raw_job_merge_row(id).await.4);
        assert!(
            list.set_job_done_owned(id, "failure-node", retry.lease_generation)
                .await
                .unwrap()
        );
        assert_eq!(
            raw_job_merge_row(id).await.0,
            FileListJobStatus::Done as i64
        );
        raw_delete_jobs(&[id]).await;
    }
}
