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

use std::{
    cmp::max,
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock as Lazy},
};

use arc_swap::ArcSwap;
use chromiumoxide::{browser::BrowserConfig, handler::viewport::Viewport};
use dotenv_config::EnvConfig;
use hashbrown::{HashMap, HashSet};
use itertools::chain;
use lettre::{
    AsyncSmtpTransport, Tokio1Executor,
    transport::smtp::{
        authentication::Credentials,
        client::{Tls, TlsParameters},
    },
};
use serde::{Deserialize, Serialize};
use sha256::digest;

use crate::{
    meta::{
        cluster,
        stream::{QueryPartitionStrategy, StreamType},
    },
    utils::sysinfo,
};

pub type FxIndexMap<K, V> = indexmap::IndexMap<K, V, ahash::RandomState>;
pub type FxIndexSet<K> = indexmap::IndexSet<K, ahash::RandomState>;
pub type RwHashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;
pub type RwHashSet<K> = dashmap::DashSet<K, ahash::RandomState>;
pub type RwAHashMap<K, V> = tokio::sync::RwLock<HashMap<K, V>>;
pub type RwAHashSet<K> = tokio::sync::RwLock<HashSet<K>>;
pub type RwBTreeMap<K, V> = tokio::sync::RwLock<BTreeMap<K, V>>;

// for DDL commands and migrations
pub const DB_SCHEMA_VERSION: u64 = 51;
pub const DB_SCHEMA_KEY: &str = "/db_schema_version/";

// global version variables
pub static VERSION: &str = env!("GIT_VERSION");
pub static COMMIT_HASH: &str = env!("GIT_COMMIT_HASH");
pub static BUILD_DATE: &str = env!("GIT_BUILD_DATE");

pub const META_ORG_ID: &str = "_meta";
pub const DEFAULT_ORG: &str = "default";

pub const MMDB_CITY_FILE_NAME: &str = "GeoLite2-City.mmdb";
pub const MMDB_ASN_FILE_NAME: &str = "GeoLite2-ASN.mmdb";
pub const GEO_IP_CITY_ENRICHMENT_TABLE: &str = "maxmind_city";
pub const GEO_IP_ASN_ENRICHMENT_TABLE: &str = "maxmind_asn";

pub const SIZE_IN_MB: f64 = 1024.0 * 1024.0;
/// Initial HTTP/2 flow-control windows (bytes) for internal gRPC channels. Apply when
/// `ZO_GRPC_HTTP2_ADAPTIVE_WINDOW=false` (default); adaptive resets to 64 KB and grows via BDP.
pub const GRPC_HTTP2_STREAM_WINDOW_SIZE: u32 = 8 * 1024 * 1024; // 8 MB
pub const GRPC_HTTP2_CONNECTION_WINDOW_SIZE: u32 = 16 * 1024 * 1024; // 16 MB
pub const SIZE_IN_GB: f64 = 1024.0 * 1024.0 * 1024.0;
// The current value is recorded in each `.vix` core file (puffin `row_group_size`
// property) so it can be changed safely without breaking row_id → row_group mapping
// for older files.
pub const PARQUET_MAX_ROW_GROUP_SIZE: usize = 128 * 1024;
pub const PARQUET_FILE_CHUNK_SIZE: usize = 100 * 1024; // 100k, num_rows
pub const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;
pub const SOURCEMAP_ZIP_MAX_SIZE: usize = 1024 * 1024 * 100; // 100 MB
// max file size for individual sourcemap. We temp cache these in mem,
// so it will affect spikes in mem at resolving stacktrace
pub const SOURCEMAP_FILE_MAX_SIZE: u64 = 1024 * 1024 * 5; // 5 MB
pub const SOURCEMAP_MEM_CACHE_SIZE: usize = 10000;

#[inline]
pub fn get_batch_size() -> usize {
    get_config().limit.batch_size
}

pub const FILE_EXT_JSON: &str = ".json";
pub const FILE_EXT_ARROW: &str = ".arrow";
pub const FILE_EXT_PARQUET: &str = ".parquet";
pub const FILE_EXT_VORTEX: &str = ".vortex";
pub const FILE_EXT_VIX: &str = ".vix";
/// The per-file INDEX SIDECAR of a `.vix` data object (format v3): same key
/// with the extension swapped, holding the inverted-index blobs. NOT a
/// file_list-tracked data file, never a merge input by itself; its size is
/// the data row's `index_size` column (`0` ⟺ no sidecar). Derive keys with
/// [`vix_sidecar_key`] — never by ad-hoc string surgery.
pub const FILE_EXT_VXI: &str = ".vxi";

/// The deterministic `.vxi` sidecar key of a `.vix` data-object key
/// (extension swapped). Callers gate on `FileMeta::index_size > 0` for
/// existence; this only derives the key. Non-`.vix` keys pass through
/// with the extension appended-swapped semantics avoided: they return
/// `None` (a sidecar exists only for core data files).
pub fn vix_sidecar_key(data_key: &str) -> Option<String> {
    data_key
        .strip_suffix(FILE_EXT_VIX)
        .map(|stem| format!("{stem}{FILE_EXT_VXI}"))
}

pub const QUERY_WITH_NO_LIMIT: i64 = -999;

pub const MINIMUM_DB_CONNECTIONS: u32 = 2;
pub const REQUIRED_DB_CONNECTIONS: u32 = 4;

// Columns added to ingested records for _INTERNAL_ use only.
pub const TIMESTAMP_COL_NAME: &str = "_timestamp";
// Used for storing and querying unflattened original data
pub const ID_COL_NAME: &str = "_o2_id";
pub const ORIGINAL_DATA_COL_NAME: &str = "_original";

pub const MESSAGE_COL_NAME: &str = "message";
pub const STREAM_NAME_LABEL: &str = "o2_stream_name";
pub const STREAM_NAME_LABEL_OLD: &str = "stream_name";
pub const DEFAULT_STREAM_NAME: &str = "default";

const _DEFAULT_SQL_FULL_TEXT_SEARCH_FIELDS: [&str; 10] = [
    "log",
    "message",
    "msg",
    "content",
    "data",
    "body",
    "json",
    "error",
    "llm_input",
    "llm_output",
];
pub static SQL_FULL_TEXT_SEARCH_FIELDS: Lazy<Vec<String>> = Lazy::new(|| {
    let cfg = get_config();
    let default_fields: &[&str] = if cfg.common.feature_default_index_fields_enabled {
        &_DEFAULT_SQL_FULL_TEXT_SEARCH_FIELDS
    } else {
        &[]
    };
    let mut fields = chain(
        default_fields.iter().map(|s| s.to_string()),
        cfg.common
            .feature_fulltext_extra_fields
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            }),
    )
    .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
});

pub static QUICK_MODEL_FIELDS: Lazy<Vec<String>> = Lazy::new(|| {
    let mut fields = get_config()
        .common
        .feature_quick_mode_fields
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
});

const _DEFAULT_DISTINCT_FIELDS: [&str; 2] = ["service_name", "operation_name"];
pub static DISTINCT_FIELDS: Lazy<Vec<String>> = Lazy::new(|| {
    let cfg = get_config();
    let default_fields: &[&str] = if cfg.common.feature_default_index_fields_enabled {
        &_DEFAULT_DISTINCT_FIELDS
    } else {
        &[]
    };
    let mut fields = chain(
        default_fields.iter().map(|s| s.to_string()),
        cfg.common
            .feature_distinct_extra_fields
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            }),
    )
    .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
});

/// Stream types whose core .vix files are COLUMN-STORE ONLY (#40): no
/// term index is built or merged for them, and queriers never probe their
/// files. Owner call 2026-08-12 for metrics: one metric family per stream,
/// low-cardinality labels, whole-window aggregations — the index is pure
/// build/merge overhead there.
pub static VIX_INDEX_DISABLED_STREAM_TYPES: Lazy<
    std::collections::HashSet<crate::meta::stream::StreamType>,
> = Lazy::new(|| {
    get_config()
        .common
        .vix_index_disabled_stream_types
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(crate::meta::stream::StreamType::from(s))
            }
        })
        .collect()
});

pub fn is_vix_index_disabled(stream_type: crate::meta::stream::StreamType) -> bool {
    VIX_INDEX_DISABLED_STREAM_TYPES.contains(&stream_type)
}

/// Stream types whose INGEST-SIDE builds (WAL move + segment L0) write
/// column-store-only core files (#42). Merge plans ignore this set — the
/// index materializes at compaction.
pub static VIX_L0_INDEX_OFF_STREAM_TYPES: Lazy<
    std::collections::HashSet<crate::meta::stream::StreamType>,
> = Lazy::new(|| {
    get_config()
        .common
        .vix_l0_index_off_stream_types
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(crate::meta::stream::StreamType::from(s))
            }
        })
        .collect()
});

pub fn is_vix_l0_index_off(stream_type: crate::meta::stream::StreamType) -> bool {
    VIX_L0_INDEX_OFF_STREAM_TYPES.contains(&stream_type)
}

pub static BLOOM_FILTER_DEFAULT_FIELDS: Lazy<Vec<String>> = Lazy::new(|| {
    let mut fields = get_config()
        .common
        .feature_bloom_filter_extra_fields
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
});

const _DEFAULT_SEARCH_AROUND_FIELDS: [&str; 6] = [
    "k8s_cluster",
    "k8s_namespace_name",
    "k8s_pod_name",
    "kubernetes_namespace_name",
    "kubernetes_pod_name",
    "hostname",
];
pub static DEFAULT_SEARCH_AROUND_FIELDS: Lazy<Vec<String>> = Lazy::new(|| {
    let mut fields = chain(
        _DEFAULT_SEARCH_AROUND_FIELDS.iter().map(|s| s.to_string()),
        get_config()
            .common
            .search_around_default_fields
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            }),
    )
    .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
});

pub static HISTOGRAM_BREAKDOWN_FIELDS: Lazy<Vec<String>> = Lazy::new(|| {
    get_config()
        .limit
        .histogram_breakdown_fields
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect()
});

pub static MEM_TABLE_INDIVIDUAL_STREAMS: Lazy<HashMap<String, usize>> = Lazy::new(|| {
    let mut map = HashMap::default();
    let streams: Vec<String> = get_config()
        .common
        .mem_table_individual_streams
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect();
    let num_mem_tables = get_config().limit.mem_table_bucket_num;
    for stream in streams.into_iter() {
        if map.contains_key(&stream) {
            continue;
        }
        map.insert(stream, num_mem_tables + map.len());
    }
    map
});

pub static COMPACT_OLD_DATA_STREAM_SET: Lazy<HashSet<String>> = Lazy::new(|| {
    get_config()
        .compact
        .old_data_streams
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect()
});

pub static NATS_KV_WATCH_MODULES: Lazy<HashSet<String>> = Lazy::new(|| {
    get_config()
        .nats
        .kv_watch_modules
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect()
});

pub static CONFIG: Lazy<ArcSwap<Config>> = Lazy::new(|| ArcSwap::from(Arc::new(init())));
static INSTANCE_ID: Lazy<RwHashMap<String, String>> = Lazy::new(Default::default);

pub fn get_config() -> Arc<Config> {
    CONFIG.load().clone()
}

pub fn refresh_config() -> Result<(), anyhow::Error> {
    CONFIG.store(Arc::new(init()));
    Ok(())
}

pub fn cache_instance_id(instance_id: &str) {
    INSTANCE_ID.insert("instance_id".to_owned(), instance_id.to_owned());
}

pub fn get_instance_id() -> String {
    match INSTANCE_ID.get("instance_id") {
        Some(id) => id.clone(),
        None => "".to_string(),
    }
}

pub fn calculate_config_file_hash(path: &PathBuf) -> Result<String, anyhow::Error> {
    let content = std::fs::read_to_string(path)?;
    Ok(digest(content))
}

pub fn load_config() -> Result<(), anyhow::Error> {
    match crate::config_path_manager::get_config_file_path() {
        Some(path) => {
            log::info!("Loading config from file {:?}", path);
            if dotenvy::from_path_override(&path).is_err() {
                return Err(anyhow::anyhow!("Config loading from file failed"));
            }
            log::info!("Config loaded successfully from file {path:?}");
        }
        None => {
            // Perform default .env discovery and set it in the config manager
            if let Ok(env_path) = dotenvy::dotenv_override() {
                log::debug!("Config init: Found .env file at {env_path:?} during boot");
                // Set the default path in config manager
                crate::config_path_manager::set_config_file_path(env_path)?;
            } else {
                return Err(anyhow::anyhow!(
                    "Config init: No .env file found during default discovery"
                ));
            }
        }
    }
    Ok(())
}
static CHROME_LAUNCHER_OPTIONS: tokio::sync::OnceCell<Option<BrowserConfig>> =
    tokio::sync::OnceCell::const_new();

pub async fn get_chrome_launch_options() -> &'static Option<BrowserConfig> {
    CHROME_LAUNCHER_OPTIONS
        .get_or_init(init_chrome_launch_options)
        .await
}

async fn init_chrome_launch_options() -> Option<BrowserConfig> {
    let cfg = get_config();
    if !cfg.chrome.chrome_enabled || !cfg.common.report_server_url.is_empty() {
        None
    } else {
        let mut browser_config = BrowserConfig::builder()
            .window_size(
                cfg.chrome.chrome_window_width,
                cfg.chrome.chrome_window_height,
            )
            .viewport(Viewport {
                width: cfg.chrome.chrome_window_width,
                height: cfg.chrome.chrome_window_height,
                device_scale_factor: Some(1.0),
                ..Viewport::default()
            });

        if cfg.chrome.chrome_with_head {
            browser_config = browser_config.with_head();
        }

        if cfg.chrome.chrome_no_sandbox {
            browser_config = browser_config.no_sandbox();
        }

        if !cfg.chrome.chrome_path.is_empty() {
            browser_config = browser_config.chrome_executable(cfg.chrome.chrome_path.as_str());
        } else {
            panic!("Chrome path must be specified");
        }
        Some(browser_config.build().unwrap())
    }
}

pub static SMTP_CLIENT: Lazy<Option<AsyncSmtpTransport<Tokio1Executor>>> = Lazy::new(|| {
    let cfg = get_config();
    if !cfg.smtp.smtp_enabled {
        None
    } else {
        let tls_parameters = TlsParameters::new(cfg.smtp.smtp_host.clone()).unwrap();
        let mut transport_builder =
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&cfg.smtp.smtp_host)
                .port(cfg.smtp.smtp_port);

        // Resolve effective TLS mode:
        // 1. If ZO_SMTP_ENCRYPTION is unset/auto, derive from port (465=ssltls, 587=starttls).
        // 2. If explicitly set, validate it against the port convention. If mismatched, log a
        //    warning and fall back to the port-derived value so the connection still works.
        let port_derived = match cfg.smtp.smtp_port {
            465 => "ssltls",
            587 | 2587 => "starttls",
            _ => "",
        };
        let effective_encryption = match cfg.smtp.smtp_encryption.as_str() {
            "" | "auto" => port_derived,
            explicit => {
                let mismatch = matches!(
                    (explicit, cfg.smtp.smtp_port),
                    ("ssltls", 587) | ("ssltls", 2587) | ("starttls", 465)
                );
                if mismatch {
                    log::warn!(
                        "[SMTP] ZO_SMTP_ENCRYPTION={explicit} conflicts with port {}; \
                         falling back to port-derived value '{port_derived}'",
                        cfg.smtp.smtp_port
                    );
                    port_derived
                } else {
                    explicit
                }
            }
        };
        transport_builder = if effective_encryption == "starttls" {
            transport_builder.tls(Tls::Required(tls_parameters))
        } else if effective_encryption == "ssltls" {
            transport_builder.tls(Tls::Wrapper(tls_parameters))
        } else {
            transport_builder
        };

        if !cfg.smtp.smtp_username.is_empty() && !cfg.smtp.smtp_password.is_empty() {
            transport_builder = transport_builder.credentials(Credentials::new(
                cfg.smtp.smtp_username.clone(),
                cfg.smtp.smtp_password.clone(),
            ));
        }
        Some(transport_builder.build())
    }
});

static SNS_CLIENT: tokio::sync::OnceCell<aws_sdk_sns::Client> = tokio::sync::OnceCell::const_new();

async fn init_sns_client() -> aws_sdk_sns::Client {
    let cfg = get_config();
    let shared_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    let sns_config = aws_sdk_sns::config::Builder::from(&shared_config)
        .endpoint_url(cfg.sns.endpoint.clone())
        .timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .connect_timeout(std::time::Duration::from_secs(cfg.sns.connect_timeout))
                .operation_timeout(std::time::Duration::from_secs(cfg.sns.operation_timeout))
                .build(),
        )
        .build();

    aws_sdk_sns::Client::from_conf(sns_config)
}

pub async fn get_sns_client() -> &'static aws_sdk_sns::Client {
    SNS_CLIENT.get_or_init(init_sns_client).await
}

pub static BLOCKED_STREAMS: Lazy<Vec<String>> = Lazy::new(|| {
    get_config()
        .common
        .blocked_streams
        .split(',')
        .map(|x| x.to_string())
        .collect()
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    #[default]
    Parquet,
    Vortex,
    /// Core-file format (v3): a `.vix` puffin DATA object carrying the
    /// records (`docs` blob) plus a `.vxi` INDEX SIDECAR (same key,
    /// extension swapped) carrying the inverted index — the sidecar exists
    /// iff the file is indexed (`FileMeta::index_size > 0`) and is never a
    /// data file itself. The unconditional format of logs/traces; never a
    /// valid value for `ZO_FILE_FORMAT`.
    Vix,
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parquet => write!(f, "parquet"),
            Self::Vortex => write!(f, "vortex"),
            Self::Vix => write!(f, "vix"),
        }
    }
}

impl std::str::FromStr for FileFormat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "parquet" => Ok(Self::Parquet),
            "vortex" => Ok(Self::Vortex),
            "vix" => Ok(Self::Vix),
            _ => Err(anyhow::anyhow!("Invalid file format: {}", s)),
        }
    }
}

impl FileFormat {
    pub fn for_ingester_stream(stream_type: StreamType, configured: Self) -> Self {
        if stream_type == StreamType::Metrics {
            Self::Parquet
        } else {
            configured
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Self::Parquet => FILE_EXT_PARQUET,
            Self::Vortex => FILE_EXT_VORTEX,
            Self::Vix => FILE_EXT_VIX,
        }
    }

    pub fn from_extension(path: &str) -> Option<Self> {
        if path.ends_with(FILE_EXT_PARQUET) {
            Some(Self::Parquet)
        } else if path.ends_with(FILE_EXT_VORTEX) {
            Some(Self::Vortex)
        } else if path.ends_with(FILE_EXT_VIX) {
            Some(Self::Vix)
        } else {
            None
        }
    }
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Config {
    pub auth: Auth,
    pub http_streaming: HttpStreaming,
    pub report_server: ReportServer,
    pub http: Http,
    pub grpc: Grpc,
    pub route: Route,
    pub common: Common,
    pub limit: Limit,
    pub compact: Compact,
    pub cache_latest_files: CacheLatestFiles,
    pub memory_cache: MemoryCache,
    pub disk_cache: DiskCache,
    pub log: Log,
    pub nats: Nats,
    pub s3: S3,
    pub sns: Sns,
    pub prom: Prometheus,
    pub smtp: Smtp,
    pub rum: RUM,
    pub chrome: Chrome,
    pub tokio_console: TokioConsole,
    pub pipeline: Pipeline,
    pub health_check: HealthCheck,
    pub enrichment_table: EnrichmentTable,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct HttpStreaming {
    #[env_config(
        name = "ZO_STREAMING_RESPONSE_CHUNK_SIZE_MB",
        default = 1,
        help = "Size in MB for each chunk when streaming search responses"
    )]
    pub streaming_response_chunk_size: usize,
    #[env_config(
        name = "ZO_STREAMING_ENABLED",
        default = true,
        help = "Enable streaming"
    )]
    pub streaming_enabled: bool,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct ReportServer {
    #[env_config(name = "ZO_ENABLE_EMBEDDED_REPORT_SERVER", default = false)]
    pub enable_report_server: bool,
    #[env_config(name = "ZO_REPORT_USER_EMAIL", default = "")]
    pub user_email: String,
    #[env_config(name = "ZO_REPORT_USER_PASSWORD", default = "")]
    pub user_password: String,
    #[env_config(name = "ZO_REPORT_SERVER_HTTP_PORT", default = 5082)]
    pub port: u16,
    #[env_config(name = "ZO_REPORT_SERVER_HTTP_ADDR", default = "127.0.0.1")]
    pub addr: String,
    #[env_config(name = "ZO_REPORT_SERVER_HTTP_IPV6_ENABLED", default = false)]
    pub ipv6_enabled: bool,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct TokioConsole {
    #[env_config(name = "ZO_TOKIO_CONSOLE_SERVER_ADDR", default = "0.0.0.0")]
    pub tokio_console_server_addr: String,
    #[env_config(name = "ZO_TOKIO_CONSOLE_SERVER_PORT", default = 6699)]
    pub tokio_console_server_port: u16,
    #[env_config(name = "ZO_TOKIO_CONSOLE_RETENTION", default = 60)]
    pub tokio_console_retention: u64,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Chrome {
    #[env_config(name = "ZO_CHROME_ENABLED", default = false)]
    pub chrome_enabled: bool,
    #[env_config(name = "ZO_CHROME_PATH", default = "")]
    pub chrome_path: String,
    #[env_config(name = "ZO_CHROME_CHECK_DEFAULT_PATH", default = true)]
    pub chrome_check_default: bool,
    #[env_config(name = "ZO_CHROME_AUTO_DOWNLOAD", default = false)]
    pub chrome_auto_download: bool,
    #[env_config(name = "ZO_CHROME_DOWNLOAD_PATH", default = "./data/download")]
    pub chrome_download_path: String,
    #[env_config(name = "ZO_CHROME_NO_SANDBOX", default = false)]
    pub chrome_no_sandbox: bool,
    #[env_config(name = "ZO_CHROME_WITH_HEAD", default = false)]
    pub chrome_with_head: bool,
    #[env_config(name = "ZO_CHROME_SLEEP_SECS", default = 20)]
    pub chrome_sleep_secs: u16,
    #[env_config(name = "ZO_CHROME_WINDOW_WIDTH", default = 1370)]
    pub chrome_window_width: u32,
    #[env_config(name = "ZO_CHROME_WINDOW_HEIGHT", default = 730)]
    pub chrome_window_height: u32,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Smtp {
    #[env_config(name = "ZO_SMTP_ENABLED", default = false)]
    pub smtp_enabled: bool,
    #[env_config(name = "ZO_SMTP_HOST", default = "localhost")]
    pub smtp_host: String,
    #[env_config(name = "ZO_SMTP_PORT", default = 25)]
    pub smtp_port: u16,
    #[env_config(name = "ZO_SMTP_USER_NAME", default = "")]
    pub smtp_username: String,
    #[env_config(name = "ZO_SMTP_PASSWORD", default = "")]
    pub smtp_password: String,
    #[env_config(name = "ZO_SMTP_REPLY_TO", default = "")]
    pub smtp_reply_to: String,
    #[env_config(name = "ZO_SMTP_FROM_EMAIL", default = "")]
    pub smtp_from_email: String,
    #[env_config(name = "ZO_SMTP_ENCRYPTION", default = "")]
    pub smtp_encryption: String,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Auth {
    #[env_config(name = "ZO_ROOT_USER_EMAIL")]
    pub root_user_email: String,
    #[env_config(name = "ZO_ROOT_USER_PASSWORD")]
    pub root_user_password: String,
    #[env_config(name = "ZO_ROOT_USER_TOKEN")]
    pub root_user_token: String,
    #[env_config(name = "ZO_CLI_USER_COOKIE")]
    pub cli_user_cookie: String,
    #[env_config(name = "ZO_COOKIE_MAX_AGE", default = 2592000)] // seconds, 30 days
    pub cookie_max_age: i64,
    #[env_config(name = "ZO_COOKIE_SAME_SITE_LAX", default = true)]
    pub cookie_same_site_lax: bool,
    #[env_config(name = "ZO_COOKIE_SECURE_ONLY", default = false)]
    pub cookie_secure_only: bool,
    #[env_config(name = "ZO_EXT_AUTH_SALT", default = "openobserve")]
    pub ext_auth_salt: String,
    #[env_config(name = "O2_ACTION_SERVER_TOKEN")]
    pub action_server_token: String,
    #[env_config(name = "ZO_SERVICE_ACCOUNT_ENABLED", default = true)]
    pub service_account_enabled: bool,
    /// Session cleanup interval in seconds (default: 3600 = 1 hour)
    /// How often to run the background job that deletes expired sessions
    #[env_config(name = "ZO_SESSION_CLEANUP_INTERVAL", default = 3600)]
    pub session_cleanup_interval: u64,
    /// Default session expiry in hours for migration (default: 24 hours)
    /// Used for existing sessions when migrating to add expires_at column
    #[env_config(name = "ZO_SESSION_DEFAULT_EXPIRY_HOURS", default = 24)]
    pub session_default_expiry_hours: i64,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Http {
    #[env_config(name = "ZO_HTTP_PORT", default = 5080)]
    pub port: u16,
    #[env_config(name = "ZO_HTTP_ADDR", default = "")]
    pub addr: String,
    #[env_config(name = "ZO_HTTP_IPV6_ENABLED", default = false)]
    pub ipv6_enabled: bool,
    #[env_config(name = "ZO_HTTP_TLS_SKIP_VERIFY", default = false)]
    pub tls_skip_verify: bool,
    #[env_config(name = "ZO_HTTP_TLS_ENABLED", default = false)]
    pub tls_enabled: bool,
    #[env_config(name = "ZO_HTTP_TLS_CERT_PATH", default = "")]
    pub tls_cert_path: String,
    #[env_config(name = "ZO_HTTP_TLS_KEY_PATH", default = "")]
    pub tls_key_path: String,
    #[env_config(name = "ZO_HTTP_TLS_MIN_VERSION", default = "", help = "Supported values: "1.2" or "1.3", default is all_version")]
    pub tls_min_version: String,
    #[env_config(
        name = "ZO_HTTP_TLS_ROOT_CERTIFICATES",
        parse,
        default = "webpki",
        help = "this value must use webpki or native. it means use standard root certificates from webpki-roots or native-roots as a rustls certificate store"
    )]
    pub tls_root_certificates: TlsRootCertificates,
    #[env_config(
        name = "ZO_HTTP_ACCESS_LOG_FORMAT",
        default = "",
        help = "Custom access log format, leave empty to use default format, shortcut: common, json"
    )]
    pub access_log_format: String,
    #[env_config(
        name = "ZO_HTTP_REAL_IP_SOURCE",
        default = "XEnvoyExternalAddress,XRealIp,RightmostXForwardedFor",
        help = "Comma-separated list of sources to resolve the real client IP; tried in \
                order, first match wins. TCP peer (ConnectInfo) is always used as the final \
                fallback. Supported entries: XEnvoyExternalAddress (Envoy/Istio), \
                XRealIp (nginx, Traefik), RightmostXForwardedFor (nginx/HAProxy/AWS ALB/GCP LB), \
                RightmostForwarded (RFC 7239), CfConnectingIp (Cloudflare), \
                TrueClientIp (Akamai/Cloudflare Enterprise), FlyClientIp (Fly.io), \
                CloudFrontViewerAddress (AWS CloudFront), ConnectInfo (TCP peer). Default \
                covers the common k8s ingresses. Only list sources whose proxy is actually \
                in front of this server; clients can spoof any header the server trusts \
                without an upstream to terminate it."
    )]
    pub real_ip_source: String,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Grpc {
    #[env_config(name = "ZO_GRPC_PORT", default = 5081)]
    pub port: u16,
    #[env_config(name = "ZO_GRPC_ADDR", default = "")]
    pub addr: String,
    #[env_config(name = "ZO_GRPC_ORG_HEADER_KEY", default = "organization")]
    pub org_header_key: String,
    #[env_config(name = "ZO_GRPC_STREAM_HEADER_KEY", default = "stream-name")]
    pub stream_header_key: String,
    #[env_config(name = "ZO_INTERNAL_GRPC_TOKEN", default = "")]
    pub internal_grpc_token: String,
    #[env_config(
        name = "ZO_GRPC_MAX_MESSAGE_SIZE",
        default = 32,
        help = "Max grpc message size in MB, default is 32 MB"
    )]
    pub max_message_size: usize,
    #[env_config(name = "ZO_GRPC_CONNECT_TIMEOUT", default = 5)] // in seconds
    pub connect_timeout: u64,
    #[env_config(name = "ZO_GRPC_CHANNEL_CACHE_DISABLED", default = false)]
    pub channel_cache_disabled: bool,
    #[env_config(
        name = "ZO_GRPC_HTTP2_ADAPTIVE_WINDOW",
        default = false,
        help = "Enable HTTP/2 adaptive (BDP-based) flow-control window growth for inter-node \
                gRPC. Off by default (fixed stream/connection windows apply). Turn on for \
                high-latency links; costs more memory under many concurrent streams."
    )]
    pub http2_adaptive_window: bool,
    #[env_config(name = "ZO_GRPC_TLS_ENABLED", default = false)]
    pub tls_enabled: bool,
    #[env_config(name = "ZO_GRPC_TLS_CERT_DOMAIN", default = "")]
    pub tls_cert_domain: String,
    #[env_config(name = "ZO_GRPC_TLS_CERT_PATH", default = "")]
    pub tls_cert_path: String,
    #[env_config(name = "ZO_GRPC_TLS_KEY_PATH", default = "")]
    pub tls_key_path: String,
    #[env_config(
        name = "ZO_GRPC_TLS_ROOT_CERTIFICATES",
        parse,
        default = "webpki",
        help = "this value can be set to webpki or native. Using webpki means client will trust a preset CA bundle. Using native means client will trust the certificates in OS trust store"
    )]
    pub tls_root_certificates: TlsRootCertificates,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsRootCertificates {
    #[default]
    Webpki,
    Native,
}

impl std::fmt::Display for TlsRootCertificates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Webpki => write!(f, "webpki"),
            Self::Native => write!(f, "native"),
        }
    }
}

impl std::str::FromStr for TlsRootCertificates {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "webpki" => Ok(Self::Webpki),
            "native" => Ok(Self::Native),
            _ => Err(anyhow::anyhow!(
                "Invalid tls_root_certificates value: '{}'. Must be 'webpki' or 'native'",
                s
            )),
        }
    }
}

#[derive(Serialize, PartialEq, Default)]
pub enum RouteDispatchStrategy {
    #[default]
    Workload,
    Random,
    Other,
}

impl std::str::FromStr for RouteDispatchStrategy {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "random" => Ok(RouteDispatchStrategy::Random),
            "workload" => Ok(RouteDispatchStrategy::default()),
            _ => Ok(RouteDispatchStrategy::Other),
        }
    }
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Route {
    #[env_config(name = "ZO_ROUTE_TIMEOUT", default = 600)]
    pub timeout: u64,
    #[env_config(name = "ZO_ROUTE_MAX_CONNECTIONS", default = 1024)]
    pub max_connections: usize,
    #[env_config(
        name = "ZO_ROUTE_MAX_RETRIES",
        default = 2,
        help = "Max number of other nodes the router will fail over to when a proxied request can't reach the selected node (e.g. during a restart/redeploy). 0 disables retry."
    )]
    pub max_retries: usize,
    #[env_config(name = "ZO_ROUTE_STRATEGY", parse, default = "workload")]
    pub dispatch_strategy: RouteDispatchStrategy,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Common {
    #[env_config(name = "ZO_APP_NAME", default = "openobserve")]
    pub app_name: String,
    #[env_config(name = "ZO_LOCAL_MODE", default = true)]
    pub local_mode: bool,
    // ZO_LOCAL_MODE_STORAGE is ignored when ZO_LOCAL_MODE is set to false
    #[env_config(name = "ZO_LOCAL_MODE_STORAGE", default = "disk")]
    pub local_mode_storage: String,
    pub is_local_storage: bool,
    #[env_config(name = "ZO_CLUSTER_COORDINATOR", default = "nats")]
    pub cluster_coordinator: String,
    #[env_config(name = "ZO_QUEUE_STORE", default = "nats")]
    pub queue_store: String,
    #[env_config(name = "ZO_META_STORE", default = "")]
    pub meta_store: String,
    #[env_config(name = "ZO_META_POSTGRES_DSN", default = "")]
    pub meta_postgres_dsn: String, // postgres://postgres:12345678@localhost:5432/openobserve
    #[env_config(name = "ZO_META_POSTGRES_RO_DSN", default = "")]
    pub meta_postgres_ro_dsn: String, // postgres://postgres:12345678@readonly:5432/openobserve
    // Individual connection vars — alternative to ZO_META_POSTGRES_DSN for environments
    // where host and password must be injected separately (e.g. ECS/K8s secrets managers).
    // Used to compose meta_postgres_dsn at startup; ignored when ZO_META_POSTGRES_DSN is set.
    #[env_config(name = "ZO_META_POSTGRES_HOST", default = "")]
    pub meta_postgres_host: String,
    #[env_config(name = "ZO_META_POSTGRES_PORT", default = 5432)]
    pub meta_postgres_port: u16,
    #[env_config(name = "ZO_META_POSTGRES_USER", default = "")]
    pub meta_postgres_user: String,
    #[env_config(name = "ZO_META_POSTGRES_PASSWORD", default = "")]
    pub meta_postgres_password: String,
    #[env_config(name = "ZO_META_POSTGRES_DBNAME", default = "")]
    pub meta_postgres_dbname: String,
    #[env_config(name = "ZO_META_DDL_DSN", default = "")]
    pub meta_ddl_dsn: String, // same db as meta store, but user with ddl perms
    #[env_config(name = "ZO_META_PARTITION_MODE", default = "auto")]
    pub meta_partition_mode: String, // "auto" or "manual"
    #[env_config(name = "ZO_NODE_ROLE", default = "all")]
    pub node_role: String,
    #[env_config(
        name = "ZO_NODE_ROLE_GROUP",
        default = "",
        help = "Role group can be empty (default), interactive, or background"
    )]
    pub node_role_group: String,
    #[env_config(name = "ZO_CLUSTER_NAME", default = "zo1")]
    pub cluster_name: String,
    #[env_config(name = "ZO_INSTANCE_NAME", default = "")]
    pub instance_name: String,
    pub instance_name_short: String,
    #[env_config(name = "ZO_INGESTION_URL", default = "")]
    pub ingestion_url: String,
    #[env_config(name = "ZO_WEB_URL", default = "http://localhost:5080")]
    pub web_url: String,
    /// Comma-separated list of extra origins allowed for CORS in addition to `web_url`.
    /// Example: `http://localhost:8081,https://staging.example.com`
    #[env_config(name = "ZO_CORS_ALLOWED_ORIGINS", default = "")]
    pub cors_allowed_origins: String,
    /// Allow alert destinations to target loopback/localhost addresses.
    /// Disabled by default (SSRF protection). Enable only in trusted environments
    /// such as CI/CD pipelines or self-hosted single-node setups where the
    /// server legitimately needs to send notifications to itself.
    #[env_config(name = "ZO_SSRF_ALLOW_LOOPBACK", default = false)]
    pub ssrf_allow_loopback: bool,
    #[env_config(name = "ZO_BASE_URI", default = "")] // /abc
    pub base_uri: String,
    #[env_config(name = "ZO_DATA_DIR", default = "./data/openobserve/")]
    pub data_dir: String,
    #[env_config(name = "ZO_DATA_WAL_DIR", default = "")] // ./data/openobserve/wal/
    pub data_wal_dir: String,
    #[env_config(name = "ZO_DATA_STREAM_DIR", default = "")] // ./data/openobserve/stream/
    pub data_stream_dir: String,
    #[env_config(name = "ZO_DATA_DB_DIR", default = "")] // ./data/openobserve/db/
    pub data_db_dir: String,
    #[env_config(name = "ZO_DATA_CACHE_DIR", default = "")] // ./data/openobserve/cache/
    pub data_cache_dir: String,
    #[env_config(name = "ZO_DATA_TMP_DIR", default = "")] // ./data/openobserve/tmp/
    pub data_tmp_dir: String,
    #[env_config(
        name = "ZO_FILE_FORMAT",
        parse,
        default = "parquet",
        help = "Flat columnar file format (parquet or vortex) for streams NOT stored as core .vix files, i.e. metrics (compactor output; ingester metrics always parquet) and internal streams. Logs/traces are always core .vix files and ignore this. 'vix' is not a valid value (normalized to parquet)"
    )]
    pub file_format: FileFormat,
    #[env_config(name = "ZO_PARQUET_COMPRESSION", default = "zstd")]
    pub parquet_compression: String,
    #[env_config(
        name = "ZO_TIMESTAMP_COMPRESSION_DISABLED",
        default = false,
        help = "Disable timestamp field compression"
    )]
    pub timestamp_compression_disabled: bool,
    #[env_config(name = "ZO_FEATURE_INGESTER_NONE_COMPRESSION", default = false)]
    pub feature_ingester_none_compression: bool,
    #[env_config(
        name = "ZO_FEATURE_SHOW_FTS_FIELD_VALUES",
        default = false,
        help = "Show field values dropdown for full text search fields in the logs page field list"
    )]
    pub show_fts_field_values: bool,
    #[env_config(
        name = "ZO_FEATURE_DEFAULT_INDEX_FIELDS_ENABLED",
        default = true,
        help = "When false, the built-in default fields for full text search and distinct values are disabled; only the fields from the *_EXTRA_FIELDS ENVs and per-stream settings are used"
    )]
    pub feature_default_index_fields_enabled: bool,
    #[env_config(name = "ZO_FEATURE_FULLTEXT_EXTRA_FIELDS", default = "")]
    pub feature_fulltext_extra_fields: String,
    #[env_config(
        name = "ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS",
        default = "",
        help = "Comma-separated fields to build bloom filter on for all streams (unioned with each stream's bloom_filter_fields setting). Core .vix files carry per-file value blooms assembled into group .bf files by the compactor; parquet-era paths keep their column blooms. Replaces the deprecated ZO_BLOOM_FILTER_DEFAULT_FIELDS"
    )]
    pub feature_bloom_filter_extra_fields: String,
    #[env_config(name = "ZO_FEATURE_DISTINCT_EXTRA_FIELDS", default = "")]
    pub feature_distinct_extra_fields: String,
    #[env_config(name = "ZO_FEATURE_QUICK_MODE_FIELDS", default = "")]
    pub feature_quick_mode_fields: String,
    // DEPRECATED since .80: the global query queue was removed in favor of
    // node-local admission (ZO_QUERY_MAX_CONCURRENCY, HTTP 429 past the
    // limit). No longer read by the OSS search path.
    #[env_config(name = "ZO_FEATURE_QUERY_QUEUE_ENABLED", default = true)]
    pub feature_query_queue_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_QUERY_PARTITION_STRATEGY",
        parse,
        default = "file_num"
    )]
    pub feature_query_partition_strategy: QueryPartitionStrategy,
    #[env_config(name = "ZO_FEATURE_QUERY_EXCLUDE_ALL", default = true)]
    pub feature_query_exclude_all: bool,
    #[env_config(name = "ZO_FEATURE_QUERY_REMOVE_FILTER_WITH_INDEX", default = true)]
    pub feature_query_remove_filter_with_index: bool,
    #[env_config(name = "ZO_FEATURE_QUERY_STREAMING_AGGS", default = true)]
    pub feature_query_streaming_aggs: bool,
    #[env_config(name = "ZO_FEATURE_JOIN_MATCH_ONE_ENABLED", default = false)]
    pub feature_join_match_one_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_JOIN_RIGHT_SIDE_MAX_ROWS",
        default = 0,
        help = "Default to 50_000 when ZO_FEATURE_JOIN_MATCH_ONE_ENABLED is true"
    )]
    pub feature_join_right_side_max_rows: usize,
    #[env_config(
        name = "ZO_FEATURE_BROADCAST_JOIN_ENABLED",
        default = true,
        help = "Enable broadcast join"
    )]
    pub feature_broadcast_join_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_BROADCAST_JOIN_LEFT_SIDE_MAX_ROWS",
        default = 0,
        help = "Max rows for left side of broadcast join, default to 10_000 rows"
    )]
    pub feature_broadcast_join_left_side_max_rows: usize,
    #[env_config(
        name = "ZO_FEATURE_BROADCAST_JOIN_LEFT_SIDE_MAX_SIZE",
        default = 0,
        help = "Max size for left side of broadcast join, default to 10 MB"
    )]
    pub feature_broadcast_join_left_side_max_size: usize, // MB
    #[env_config(
        name = "ZO_FEATURE_ENRICHMENT_BROADCAST_JOIN_ENABLED",
        default = true,
        help = "Enable enrichment table broadcast join"
    )]
    pub feature_enrichment_broadcast_join_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_PUSHDOWN_FILTER_ENABLED",
        default = true,
        help = "Enable pushdown filter"
    )]
    pub feature_pushdown_filter_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_DYNAMIC_PUSHDOWN_FILTER_ENABLED",
        default = true,
        help = "Enable dynamic pushdown filter"
    )]
    pub feature_dynamic_pushdown_filter_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_SINGLE_NODE_OPTIMIZE_ENABLED",
        default = true,
        help = "Enable single node optimize(used for debug, not document)"
    )]
    pub feature_single_node_optimize_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_QUERY_SKIP_WAL",
        default = false,
        help = "Skip WAL for query"
    )]
    pub feature_query_skip_wal: bool,
    #[env_config(
        name = "ZO_FEATURE_PARTIAL_REDUCE_ENABLED",
        default = true,
        help = "Enable partial reduce aggregation to reduce data transfer to the leader"
    )]
    pub feature_partial_reduce_enabled: bool,
    #[env_config(
        name = "ZO_FEATURE_SHARED_MEMTABLE_ENABLED",
        default = false,
        help = "Enable shared memtable across multiple organizations"
    )]
    pub feature_shared_memtable_enabled: bool,
    #[env_config(name = "ZO_UI_ENABLED", default = true)]
    pub ui_enabled: bool,
    #[env_config(name = "ZO_UI_SQL_BASE64_ENABLED", default = false)]
    pub ui_sql_base64_enabled: bool,
    #[env_config(
        name = "ZO_DEFAULT_THEME_LIGHT_MODE_COLOR",
        default = "",
        help = "Default theme color for light mode. If not set, uses application default."
    )]
    pub default_theme_light_mode_color: String,
    #[env_config(
        name = "ZO_DEFAULT_THEME_DARK_MODE_COLOR",
        default = "",
        help = "Default theme color for dark mode. If not set, uses application default."
    )]
    pub default_theme_dark_mode_color: String,
    #[env_config(name = "ZO_METRICS_DEDUP_ENABLED", default = true)]
    pub metrics_dedup_enabled: bool,
    #[env_config(
        name = "ZO_BLOOM_FILTER_ENABLED",
        default = true,
        help = "Use bloom filters when searching parquet files (legacy data and WAL). Core .vix files carry their own term index and are unaffected"
    )]
    pub bloom_filter_enabled: bool,
    #[env_config(
        name = "ZO_BLOOM_FILTER_PARQUET_ENABLED",
        default = false,
        help = "Write per-column bloom filters into parquet files (WAL and non-core streams such as metrics). Not applicable to core .vix files"
    )]
    pub bloom_filter_parquet_enabled: bool,
    #[deprecated(
        since = "0.92.0",
        note = "Please use `ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS` instead. This ENV will be removed in v1.0.0"
    )]
    #[env_config(name = "ZO_BLOOM_FILTER_DEFAULT_FIELDS", default = "")]
    pub bloom_filter_default_fields: String,
    #[env_config(
        name = "ZO_SEARCH_AROUND_DEFAULT_FIELDS",
        default = "",
        help = "Comma separated list of fields to use for search around"
    )]
    pub search_around_default_fields: String,
    #[env_config(
        name = "ZO_WAL_FSYNC_DISABLED",
        default = true,
        help = "Skip fsync on WAL appends. Kept on by default because the logs and traces ingest paths pass it straight through as the per-request fsync flag, so turning fsync on costs one fsync per ingest request, taken while holding the per-writer WAL lock. Exposure while on: acked rows that only exist in the WAL survive a process crash (the bytes are in the page cache) but not a power loss or kernel panic, until the memtable behind them is persisted -- worst case ZO_MAX_FILE_RETENTION_TIME (rotation) plus ZO_MEM_PERSIST_INTERVAL. This knob does NOT weaken the persist chain: wal rotation, shutdown, the .par files, the .lock file and their directories are fsynced unconditionally, so a parquet file that exists is always durable and a deleted wal file was always replaced by one."
    )]
    pub wal_fsync_disabled: bool,
    #[env_config(
        name = "ZO_INGEST_SEGMENT_MODE",
        default = false,
        help = "S3-first ingest: buffer rows in memory and ship one multi-stream segment object per node per flush interval instead of the local memtable/WAL/mover pipeline (DESIGN-SEGMENT-WAL.md). Ack-on-append: a node crash may lose up to one flush interval of acked data. Enable only after every node in the fleet runs a segment-aware build — older followers silently ignore leader-assigned segments."
    )]
    pub ingest_segment_mode: bool,
    #[env_config(
        name = "ZO_SEGMENT_FLUSH_INTERVAL_MS",
        default = 1000,
        help = "Segment WAL: max time rows wait in the node buffer before the segment object is shipped"
    )]
    pub segment_flush_interval_ms: u64,
    #[env_config(
        name = "ZO_SEGMENT_FLUSH_SIZE_MB",
        default = 64,
        help = "Segment WAL: buffered arrow bytes that trigger an early segment flush"
    )]
    pub segment_flush_size_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUFFER_MAX_MB",
        default = 512,
        help = "Segment WAL: hard cap on buffered bytes; appends beyond it are rejected with 503 (honest backpressure while object storage is slow or down). Sized to absorb object-store hiccups at full inbound — the 128MB initial default browned out prod ingest (2026-07-31)."
    )]
    pub segment_buffer_max_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_RETAIN_SECS",
        default = 3600,
        help = "Segment WAL: how long built segments stay queryable/deletable-after before the sweeper removes them"
    )]
    pub segment_retain_secs: u64,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_BATCH",
        default = 32,
        help = "Segment WAL: max segments one builder claim processes into L0 files. Bigger \
                claims mean fewer, larger per-(stream, hour) L0 files (capped independently \
                by ZO_SEGMENT_BUILD_CHUNK_MB), at the cost of the whole claim's decoded \
                Arrow held in RAM through the build: budget ~ batch x ZO_SEGMENT_FLUSH_SIZE_MB"
    )]
    pub segment_build_batch: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_CLAIM_MB",
        default = 0,
        help = "Segment WAL (#47): size builder claims by BYTES instead of a fixed segment \
                count — the effective batch becomes clamp(budget / live-average segment size, \
                4, 256), adapting to segment-size variance (fat vpc-flow segments claim fewer, \
                thin metrics segments claim more) while the decode-memory bound stays this \
                budget. 0 = disabled, ZO_SEGMENT_BUILD_BATCH's fixed count applies. The \
                all-or-nothing floor (#44) and the ZO_SEGMENT_BUILD_MAX_WAIT_SECS age escape \
                apply identically in both modes."
    )]
    pub segment_build_claim_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_CHUNK_MB",
        default = 128,
        help = "Segment WAL: maximum decoded Arrow MiB accumulated into one direct L0 build \
                for a single (stream, actual timestamp hour) inside one contiguous segment-ID \
                run. The cap is applied only between whole per-segment/hour contributions, so \
                an oversized contribution still builds alone. Floor 1 MiB."
    )]
    pub segment_build_chunk_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_CONCURRENCY",
        default = 16,
        help = "M12/M17: concurrent small stream-chunk L0 builds per claim. Since M17 this \
                is the SECONDARY (count) cap — ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB is the \
                binding control (each build reserves its decoded input bytes before \
                starting), which is why the default rose 3 -> 16: builds only run wide \
                when the byte budget proves they fit. Floor 1."
    )]
    pub segment_build_concurrency: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_UPLOAD_CONCURRENCY",
        default = 8,
        help = "Segment WAL: maximum L0 files uploaded concurrently after their planned keys \
                are durable. Each file preserves DATA-before-INDEX ordering; the count is a \
                secondary cap under ZO_SEGMENT_BUILD_UPLOAD_MAX_INFLIGHT_MB. Floor 1."
    )]
    pub segment_build_upload_concurrency: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_UPLOAD_MAX_INFLIGHT_MB",
        default = 256,
        help = "Segment WAL: process-local MiB budget for L0 payloads in concurrent object-store \
                PUTs. Admission counts DATA plus the optional INDEX sidecar; one oversized file \
                takes the whole budget and still runs, preventing deadlock. Floor 1 MiB."
    )]
    pub segment_build_upload_max_inflight_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB",
        default = 0,
        help = "M17: process-wide DECODED-byte budget for L0 build admission — replaces the \
                count-knob treadmill (batch/superbatch/concurrency retunes per traffic \
                shape). A claimed batch reserves its estimated decoded bytes before \
                fetch+decode (segment meta size x an inflation EMA seeded at 5.0, corrected \
                to post-decode actuals), and each stream-chunk build reserves its actual \
                decoded input bytes before running; a reservation that cannot fit waits, \
                but ONE claim and ONE build always admit (nothing deadlocks on an \
                oversized unit). 0 = auto: 40% of the detected container/cgroup memory."
    )]
    pub segment_build_memory_budget_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_FETCH_DECODE_CONCURRENCY",
        default = 2,
        help = "M13 (1c): segment objects fetched+decoded concurrently per claimed batch \
                (was a hardcoded 2). Memory scales with in-flight DECODED objects (each \
                payload decompresses to ~ZO_SEGMENT_FLUSH_SIZE_MB of arrow) — pair with the \
                decoded-bytes build caps; a ~512MB super-batch fetching its ~130 objects \
                two at a time made this THE drain-rate limiter once claim-side waits were \
                removed. Dedicated builder/compactor pods can run 8; ingesters stay low. \
                Floor 1."
    )]
    pub segment_fetch_decode_concurrency: usize,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_MAX_WAIT_SECS",
        default = 15,
        help = "Segment WAL: builders wait for a full ZO_SEGMENT_BUILD_BATCH before claiming, up to this many seconds past the oldest claimable segment's registration. Turns the per-claim L0 output into full batches instead of 1-2-segment slivers (10k tiny files/hour/stream in prod, 2026-08-07). Data stays queryable through the segment tail while it waits. 0 = claim immediately (legacy behavior)"
    )]
    pub segment_build_max_wait_secs: u64,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_AGE_LANE_SECS",
        default = 21600,
        help = "Segment WAL aging lane (M13): once the OLDEST claimable segment is older than \
                this many seconds, builders reserve a fraction of claim passes \
                (ZO_SEGMENT_BUILD_AGE_LANE_RATIO) that scan OLDEST-first, so a standing \
                backlog at balanced capacity can never starve the oldest cohort into the \
                raw-object S3 lifecycle expiry (prod 2026-08-18: oldest pending stuck 15+ \
                hours under a 74.5k backlog of newest-first claims). Modeled on \
                ZO_COMPACT_LIVE_JOB_NUM's reserved-lane design; steady-state claiming is \
                untouched while the lane is disengaged. 0 disables the lane."
    )]
    pub segment_build_age_lane_secs: u64,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_AGE_LANE_RATIO",
        default = 0.25,
        help = "Fraction of builder claim passes that claim OLDEST-first while the aging \
                lane is engaged (0.25 = every 4th pass, 1.0 = every pass). Clamped to \
                [0.0, 1.0] at load; 0 disables the lane."
    )]
    pub segment_build_age_lane_ratio: f64,
    #[env_config(
        name = "ZO_SEGMENT_LATE_LANE_HOURS",
        default = 0,
        help = "M31a: the LATE LANE master switch. Rows whose hour partition is at \
                least this many hours behind the current hour are LATE (late-arriving \
                spans/logs): at ingest they buffer separately and ship as ALL-LATE \
                segments (classifier: created_at - max_ts, no schema change), and the \
                builder holds those segments from claiming until \
                ZO_SEGMENT_LATE_CLAIM_HOLD_SECS so one build wave coalesces the whole \
                fleet's late rows into one L0 file per (stream, hour) per wave — \
                instead of one 1-record ~3KB file per hour per build batch (prod \
                measured ~3.5k such files/h on traces alone, 85% into old hours). \
                Rows stay queryable through the segment tail the entire hold. 0 = OFF \
                (exact pre-M31a behavior). Sane value: 2 (the previous hour is NOT \
                late — hour-boundary rows keep today's path)."
    )]
    pub segment_late_lane_hours: usize,
    #[env_config(
        name = "ZO_SEGMENT_LATE_FLUSH_SIZE_MB",
        default = 8,
        help = "M31a: byte trigger of the LATE sub-buffer (late frames accumulate \
                across flush ticks and ship as their own segment when this fills or \
                the age trigger below fires). Counts toward ZO_SEGMENT_BUFFER_MAX_MB."
    )]
    pub segment_late_flush_size_mb: usize,
    #[env_config(
        name = "ZO_SEGMENT_LATE_FLUSH_MAX_SECS",
        default = 30,
        help = "M31a: age trigger of the LATE sub-buffer. NOTE the deliberate \
                durability trade, called out per the owner's crash-window contract: \
                LATE rows (already ≥ ZO_SEGMENT_LATE_LANE_HOURS old) may sit in \
                memory up to this long before shipping, vs ~ZO_SEGMENT_FLUSH_INTERVAL_MS \
                for fresh rows — a node crash loses up to this window of LATE rows \
                only. Fresh-row durability is unchanged."
    )]
    pub segment_late_flush_max_secs: u64,
    #[env_config(
        name = "ZO_SEGMENT_LATE_CLAIM_HOLD_SECS",
        default = 900,
        help = "M31a: all-late segments become claimable only this many seconds after \
                creation — the hold is what batches a fleet's late rows into one \
                build wave. Must stay far under the raw-object S3 lifecycle (1d prod; \
                the aging lane is the deeper backstop). Late segments stay query-\
                visible through the segment tail while held."
    )]
    pub segment_late_claim_hold_secs: u64,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_LEASE_SECS",
        default = 120,
        help = "Segment WAL: builder lease; a claim whose heartbeat is older than this is re-claimable"
    )]
    pub segment_build_lease_secs: u64,
    #[env_config(
        name = "ZO_SEGMENT_BUILD_404_TOMBSTONE",
        default = true,
        help = "M29: a claimed wal_segment whose object GET returns NotFound (the S3 \
                lifecycle expired it — S3 reads are strongly consistent, so the data is \
                gone for good) is terminally resolved: the row flips Built with no files \
                and the sweeper retires it, instead of retrying the 404 on every lease \
                expiry forever. Kill-era zombie rows otherwise recycle through EVERY \
                claim batch and dilute real segments into per-stream sliver L0 files \
                (prod 2026-08-24: 189.7k zombie rows, 722k 404-skips/30m, claim batches \
                of 64 with 1 real segment emitting 7 sliver files). false = legacy \
                skip-and-retry."
    )]
    pub segment_build_404_tombstone: bool,
    #[env_config(
        name = "ZO_SEGMENT_SCAN_FETCH_CONCURRENCY",
        default = 32,
        help = "Segment WAL: concurrent cache/object-store reads per live-tail query. Fetching is pipelined separately from decode so small-object latency does not serialize behind zstd work."
    )]
    pub segment_scan_fetch_concurrency: usize,
    #[env_config(
        name = "ZO_SEGMENT_SCAN_DECODE_CONCURRENCY",
        default = 8,
        help = "Segment WAL: concurrent blocking zstd/CRC decodes per live-tail query. The regular ordered top-n path also uses this as its bounded decode wave size."
    )]
    pub segment_scan_decode_concurrency: usize,
    #[env_config(
        name = "ZO_WAL_WRITE_QUEUE_ENABLED",
        default = false,
        help = "Route WAL writes through a per-writer queue. The ingest request still waits for its own write to complete -- the consumer reports the write's outcome back before the request is acked -- so this smooths bursts across requests without weakening ack truth: an enqueue is never acked as a durable write, and a consumer failure surfaces as the request's error instead of a log line."
    )]
    pub wal_write_queue_enabled: bool,
    #[env_config(
        name = "ZO_WAL_WRITE_QUEUE_FULL_REJECT",
        default = false,
        help = "Reject write when write queue is full"
    )]
    pub wal_write_queue_full_reject: bool,
    #[env_config(
        name = "ZO_WAL_DEDICATED_RUNTIME_ENABLED",
        default = false,
        help = "Enable dedicated runtime with CPU binding for WAL writer threads"
    )]
    pub wal_dedicated_runtime_enabled: bool,
    #[env_config(name = "ZO_TRACING_ENABLED", default = false)]
    pub tracing_enabled: bool,
    #[env_config(name = "ZO_TRACING_SEARCH_ENABLED", default = false)]
    pub tracing_search_enabled: bool,
    #[env_config(name = "OTEL_OTLP_HTTP_ENDPOINT", default = "")]
    pub otel_otlp_url: String,
    #[env_config(name = "OTEL_OTLP_GRPC_ENDPOINT", default = "")]
    pub otel_otlp_grpc_url: String,
    #[env_config(
        name = "ZO_TRACING_GRPC_ORGANIZATION",
        default = "",
        help = "Used in metadata when exporting traces to grpc endpoint."
    )]
    pub tracing_grpc_header_org: String,
    #[env_config(
        name = "ZO_TRACING_GRPC_STREAM_NAME",
        default = "",
        help = "Used in metadata when exporting traces to grpc endpoint."
    )]
    pub tracing_grpc_header_stream_name: String,
    #[env_config(name = "ZO_TRACING_HEADER_KEY", default = "Authorization")]
    pub tracing_header_key: String,
    #[env_config(
        name = "ZO_TRACING_HEADER_VALUE",
        default = "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM="
    )]
    pub tracing_header_value: String,
    #[env_config(
        name = "ZO_TRACING_EXTRA_ENVS",
        default = "",
        help = "Comma-separated list of environment variable names to include as resource attributes in traces."
    )]
    pub tracing_extra_envs: String,
    #[env_config(name = "ZO_TELEMETRY", default = true)]
    pub telemetry_enabled: bool,
    #[env_config(name = "ZO_TELEMETRY_URL", default = "https://e1.zinclabs.dev")]
    pub telemetry_url: String,
    #[env_config(name = "ZO_TELEMETRY_HEARTBEAT", default = 1800)] // seconds
    pub telemetry_heartbeat: i64,
    #[env_config(name = "ZO_KEYEVENT_TELEMETRY_URL", default = "")]
    pub keyevent_telemetry_url: String,
    #[env_config(name = "ZO_PROMETHEUS_ENABLED", default = true)]
    pub prometheus_enabled: bool,
    #[env_config(name = "ZO_PRINT_KEY_CONFIG", default = false)]
    pub print_key_config: bool,
    #[env_config(name = "ZO_PRINT_KEY_EVENT", default = false)]
    pub print_key_event: bool,
    #[env_config(name = "ZO_PRINT_KEY_SQL", default = true)]
    pub print_key_sql: bool,
    #[env_config(name = "ZO_PRINT_PLAN_SINGLE_LINE", default = true)]
    pub print_plan_single_line: bool,
    // usage reporting
    #[env_config(name = "ZO_USAGE_REPORTING_ENABLED", default = false)]
    pub usage_enabled: bool,
    #[env_config(
        name = "ZO_USAGE_REPORTING_MODE",
        default = "local",
        help = "possible values - 'local', 'remote', 'both'"
    )] // local, remote, both
    pub usage_reporting_mode: String,
    #[env_config(
        name = "ZO_USAGE_REPORTING_URL",
        default = "http://localhost:5080/api/_meta/usage/_json"
    )]
    pub usage_reporting_url: String,
    #[env_config(name = "ZO_USAGE_REPORTING_CREDS", default = "")]
    pub usage_reporting_creds: String,
    #[env_config(name = "ZO_USAGE_REPORTING_ERRORS_ENABLED", default = true)]
    pub usage_reporting_errors_enabled: bool,
    #[env_config(name = "ZO_USAGE_BATCH_SIZE", default = 2000)]
    pub usage_batch_size: usize,
    #[env_config(
        name = "ZO_USAGE_PUBLISH_INTERVAL",
        default = 60,
        help = "duration in seconds after last reporting usage will be published"
    )]
    // in seconds
    pub usage_publish_interval: i64,
    #[env_config(
        name = "ZO_ERROR_PUBLISH_TIMEOUT_SECS",
        default = 2,
        help = "timeout in seconds for publishing error data to self-reporting queue"
    )]
    pub error_publish_timeout_secs: u64,
    // MMDB
    #[env_config(name = "ZO_MMDB_DATA_DIR")] // ./data/openobserve/mmdb/
    pub mmdb_data_dir: String,
    #[env_config(name = "ZO_MMDB_DISABLE_DOWNLOAD", default = false)]
    pub mmdb_disable_download: bool,
    #[env_config(name = "ZO_MMDB_UPDATE_DURATION_DAYS", default = 30)] // default 30 days
    pub mmdb_update_duration_days: u64,
    #[env_config(
        name = "ZO_MMDB_GEOLITE_CITYDB_URL",
        default = "https://geoip.zinclabs.dev/GeoLite2-City.mmdb"
    )]
    pub mmdb_geolite_citydb_url: String,
    #[env_config(
        name = "ZO_MMDB_GEOLITE_ASNDB_URL",
        default = "https://geoip.zinclabs.dev/GeoLite2-ASN.mmdb"
    )]
    pub mmdb_geolite_asndb_url: String,
    #[env_config(
        name = "ZO_MMDB_GEOLITE_CITYDB_SHA256_URL",
        default = "https://geoip.zinclabs.dev/GeoLite2-City.sha256"
    )]
    pub mmdb_geolite_citydb_sha256_url: String,
    #[env_config(
        name = "ZO_MMDB_GEOLITE_ASNDB_SHA256_URL",
        default = "https://geoip.zinclabs.dev/GeoLite2-ASN.sha256"
    )]
    pub mmdb_geolite_asndb_sha256_url: String,
    #[env_config(name = "ZO_DEFAULT_SCRAPE_INTERVAL", default = 15)]
    // Default scrape_interval value 15s
    pub default_scrape_interval: u32,
    #[env_config(name = "ZO_MEMORY_CIRCUIT_BREAKER_ENABLED", default = false)]
    pub memory_circuit_breaker_enabled: bool,
    #[env_config(name = "ZO_MEMORY_CIRCUIT_BREAKER_RATIO", default = 90)]
    pub memory_circuit_breaker_ratio: usize,
    #[env_config(name = "ZO_DISK_CIRCUIT_BREAKER_ENABLED", default = false)]
    pub disk_circuit_breaker_enabled: bool,
    #[env_config(
        name = "ZO_DISK_CIRCUIT_BREAKER_THRESHOLD",
        default = 90,
        help = "Disk space threshold. Values < 100 are treated as percentage of total disk space used (e.g., 90 = trigger at 90% usage), values >= 100 are treated as absolute MB of required free space"
    )]
    pub disk_circuit_breaker_threshold: usize,
    #[env_config(
        name = "ZO_INGEST_ADMISSION_ENABLED",
        default = true,
        help = "Gate ingest HTTP requests on the memory envelope BEFORE the request body is buffered/decoded. Uses Content-Length to project the in-process transient and meters it against the memory circuit breaker envelope. Envelope gating requires the memory circuit breaker to be enabled; early 413 for oversized Content-Length works regardless."
    )]
    pub ingest_admission_enabled: bool,
    #[env_config(
        name = "ZO_INGEST_ADMISSION_EXPANSION_FACTOR",
        default = 6,
        help = "Projected in-process expansion multiple for an uncompressed ingest body (body buffer + decode + json + arrow transient). projected = content_length * factor."
    )]
    pub ingest_admission_expansion_factor: usize,
    #[env_config(
        name = "ZO_INGEST_ADMISSION_COMPRESSED_FACTOR",
        default = 30,
        help = "Projected in-process expansion multiple for a compressed (gzip/deflate/br/snappy) ingest body, covering decompression plus decode expansion. projected = content_length * factor."
    )]
    pub ingest_admission_compressed_factor: usize,
    #[env_config(
        name = "ZO_RESTRICTED_ROUTES_ON_EMPTY_DATA",
        default = false,
        help = "Control the redirection of a user to ingestion page in case there is no stream found."
    )]
    pub restricted_routes_on_empty_data: bool,
    #[env_config(
        name = "ZO_ENABLE_INVERTED_INDEX",
        default = true,
        help = "Toggle inverted index generation."
    )]
    pub inverted_index_enabled: bool,
    #[env_config(
        name = "ZO_WAL_NARROW_SCHEMA",
        default = true,
        help = "Ingest batches carry only the fields present in the data (plus _timestamp) \
                instead of the full stream schema: WAL arrow-IPC bytes, memtable footprint \
                and persist width all scale with the data, not the stream-schema union. The \
                memtable/persist/search/replay paths adapt heterogeneous batch schemas \
                natively. Default TRUE since 2026-08-12 (owner call; both fleet envs had \
                pinned it true since .26/.28 with no regressions) — false remains as the \
                rollback lever."
    )]
    pub wal_narrow_schema: bool,
    #[env_config(
        name = "ZO_INGEST_CANONICAL_SCHEMA",
        default = false,
        help = "Pin each stream field to the first non-null type successfully registered in the schema store and cast later scalar values to that type before alerts, partitioning, distinct extraction, and WAL writes. Existing stream field types are the rollout baseline. Failed scalar casts become null. Keep false until every writer in a mixed-version deployment understands the pinned-type policy."
    )]
    pub ingest_canonical_schema: bool,
    #[env_config(
        name = "ZO_VIX_FULL_SCAN_RANGED_MIN_BYTES",
        default = 268435456,
        help = "Wide core-file full scans that request _source switch to chunk-granular ranged \
                reads when the object is at least this many bytes, instead of buffering the \
                whole compressed blob in RAM. Row selections and projections without _source \
                always use ranged reads. 0 keeps whole-object gets only for small wide scans."
    )]
    pub vix_full_scan_ranged_min_bytes: usize,
    #[env_config(
        name = "ZO_VIX_EVAL_BAIL_BYTES",
        default = 536870912,
        help = "Give up an index-optimizer evaluation when its PROJECTED total fetch volume \
                (bytes fetched so far / files evaluated × files total, sampled after 32 files) \
                exceeds this many bytes, handing the remaining files to the columnar scan with \
                the filter added back. Low-selectivity conditions cost more through the index \
                than through the scan. 0 disables the bail-out."
    )]
    pub vix_eval_bail_bytes: usize,
    /// Move-job builds whose input WAL original bytes meet this spool the
    /// finished .vix container to `<wal>/vix_spool` and upload from the
    /// path instead of holding the whole container (plus its upload clone)
    /// in memory. 0 disables spooling.
    #[env_config(name = "ZO_VIX_MOVE_SPOOL_MIN_BYTES", default = 268435456)]
    pub vix_move_spool_min_bytes: usize,
    /// Threads decoding ONE file's chunks during a full (non-selected)
    /// vix scan; 0 = single-threaded (the default — cross-file concurrency
    /// usually saturates cores first, raise for few-big-files scans).
    #[env_config(name = "ZO_VIX_SCAN_DECODE_THREADS", default = 0)]
    pub vix_scan_decode_threads: usize,
    #[env_config(
        name = "ZO_VIX_ORDER_MERGE_MAX_REGIONS",
        default = 64,
        help = "Concat-order .vix files with at most this many proven desc regions serve \
                ORDER BY _timestamp DESC through the k-way region merge (declared sort, \
                no SortExec); wider files fall back to the real sort. Each OPEN region \
                costs one decoded chunk window; lazy opening keeps the usual count near 1. \
                0 disables merged ordered reads (every concat file sorts)."
    )]
    pub vix_order_merge_max_regions: usize,
    #[env_config(
        name = "ZO_VIX_POSTINGS_CHUNK_BYTES",
        default = 131072,
        help = "Target chunk size for the postings column in .vix index files."
    )]
    pub vix_postings_chunk_bytes: usize,
    #[env_config(
        name = "ZO_VIX_BLOOM_FPP",
        default = 0.001,
        help = "False-positive probability of per-file value blooms in .vix files (needle \
                queries probe hundreds of files; each false positive costs a dictionary walk)."
    )]
    pub vix_bloom_fpp: f64,
    #[env_config(
        name = "ZO_VIX_FETCH_CONCURRENCY",
        default = 16,
        help = "Global cap on in-flight .vix range fetches per process. Queue wait does NOT \
                count toward ZO_VIX_FETCH_TIMEOUT — the timeout bounds only the active fetch, \
                so wide eval fan-out cannot manufacture spurious timeouts (whose fallback \
                converts index-answerable queries into full scans). 0 = uncapped."
    )]
    pub vix_fetch_concurrency: usize,
    #[env_config(
        name = "ZO_VIX_QUERY_PREFETCH",
        default = true,
        help = "Cold-open prefetch for ranged vix index evaluation (M14): before a file \
                group is evaluated, files WITHOUT a memoized reader batch-fetch their \
                eager footer tails (data object + index sidecar, the \
                ZO_VIX_EAGER_TAIL_BYTES window) in one bounded-concurrency wave, so a \
                multi-file cold query pays one parallel fetch ROUND instead of per-file \
                sequential open rounds. Wave fetches respect ZO_VIX_FETCH_CONCURRENCY \
                and count toward the ZO_VIX_EVAL_BAIL_BYTES budget. false = open lazily \
                per file (the pre-M14 behavior)."
    )]
    pub vix_query_prefetch: bool,
    #[env_config(
        name = "ZO_VIX_PLIST_MIN_DOCS",
        default = 0,
        help = "Terms with at least this many matching docs store postings out-of-row with a \
                skip table (rank-based eval skips decoding dense lists). 0 = off. Enable ONLY \
                after every reader in the fleet ships pointer-cell support (#15 rollout)."
    )]
    pub vix_plist_min_docs: usize,
    #[env_config(
        name = "ZO_VIX_INDEX_DISABLED_STREAM_TYPES",
        default = "metrics",
        help = "Comma-separated stream types whose core .vix files are COLUMN-STORE ONLY (#40): \
                no term index built or merged; every schema field materializes as a docs \
                column; queriers route those streams straight to the columnar scan."
    )]
    pub vix_index_disabled_stream_types: String,
    #[env_config(
        name = "ZO_VIX_L0_INDEX_OFF_STREAM_TYPES",
        default = "",
        help = "Comma-separated stream types whose ingest-side builds (the WAL move job and \
                the segment L0 builder) write COLUMN-STORE-ONLY core files (#42 hot-data \
                mode): no term index is built at L0 — every present field materializes as a \
                docs column and recent-window filters run columnar — and the index appears \
                when compaction merges/heals the files (merge plans ignore this list and \
                keep indexing per ZO_VIX_INDEX_DISABLED_STREAM_TYPES). DEFAULT EMPTY (off). \
                Enable only when EVERY querier runs the index-off read guards (.88+): a \
                pre-guard reader treats an index-off file's empty dictionary as proof of \
                field absence and silently drops rows."
    )]
    pub vix_l0_index_off_stream_types: String,
    #[env_config(
        name = "ZO_VIX_METRICS_CORE_FILE_ENABLED",
        default = false,
        help = "Activation switch for metrics streams writing core .vix files at all \
                (column-store-only per ZO_VIX_INDEX_DISABLED_STREAM_TYPES). DEFAULT OFF: flip \
                only after EVERY querier in the fleet runs the index-off read guards (#15 \
                rollout discipline) — a pre-guard reader treats an index-off file's empty \
                dictionary as proof of field absence and silently drops rows."
    )]
    pub vix_metrics_core_file_enabled: bool,
    #[env_config(
        name = "ZO_VIX_BLOOM_COMPOSITE",
        default = true,
        help = "#48: per-file COMPOSITE value bloom — one reserved section keyed by \
                {field name}\\0{value} over EVERY term field, making equality on ANY field \
                bloom-decidable (file-skip pruning over multi-day windows with ~8KiB reads per \
                256-file .bf group). Adds one hash per distinct term at build/merge time and \
                ~2 bytes per distinct term of blob size at the default FPP. Readers that \
                predate the section ignore it; the pruner keeps files whose .bf lacks it \
                (fail-open) — safe to enable per-side in any order. DEFAULT ON since v2 M7 \
                (it is what serves equality on #52 bloom-only-demoted fields); set false to \
                go dark."
    )]
    pub vix_bloom_composite: bool,
    #[env_config(
        name = "ZO_VIX_BLOOM_ONLY_FIELDS",
        default = "",
        help = "Comma list of STRING fields demoted from the term index to bloom-only \
                (#52): no dictionary/postings; values land in the composite bloom and \
                equality queries take file-level bloom pruning + in-file column scan. \
                For high-cardinality IDs (trace_id/span_id) this removes the most \
                expensive part of the index build."
    )]
    pub vix_bloom_only_fields: String,
    #[env_config(
        name = "ZO_VIX_BLOOM_ONLY_NEVER",
        default = "",
        help = "Comma list of fields NEVER auto-demoted to bloom-only (keeps prefix \
                search / index serving for fields the auto ratio would otherwise demote)."
    )]
    pub vix_bloom_only_never: String,
    #[env_config(
        name = "ZO_VIX_BLOOM_ONLY_AUTO_RATIO",
        default = 0.5,
        help = "AUTO bloom-only demotion threshold (#52): a string term field whose \
                distinct-value/row ratio is >= this (and clears \
                ZO_VIX_BLOOM_ONLY_MIN_DISTINCT) is written bloom-only. ONE rule, applied \
                at BOTH write sites since v2 M7: merge plans count the input dictionaries, \
                and first-encode builds (the move job) count the writer's own term map — \
                ID-shaped fields (trace_id/span_id) are demoted from birth. The marker is \
                sticky across merges; ZO_VIX_BLOOM_ONLY_NEVER + a heal un-demotes. \
                DEFAULT 0.5 (the dev-proven value); 0 disables auto entirely."
    )]
    pub vix_bloom_only_auto_ratio: f64,
    #[env_config(
        name = "ZO_VIX_BLOOM_ONLY_MIN_DISTINCT",
        default = 65536,
        help = "Absolute distinct-term floor for the auto bloom-only demotion — small \
                files' noisy ratios must not demote real fields."
    )]
    pub vix_bloom_only_min_distinct: u64,
    #[env_config(
        name = "ZO_L0_SUPERBATCH_MB",
        default = 512,
        help = "#54: builders CONCATENATE consecutive full segment claims until this \
                many megabytes (or the age cap) and build them as ONE batch — each \
                touched (stream, hour) becomes one L0 file instead of one per claim. \
                0 restores per-claim builds. Bounds the builder's decoded-frame memory."
    )]
    pub segment_build_superbatch_mb: usize,
    #[env_config(
        name = "ZO_L0_SUPERBATCH_MAX_SECS",
        default = 120,
        help = "#54: age cap on super-batch accumulation — also the crash-replay bound \
                (an unfinished super-batch re-pends whole via lease expiry)."
    )]
    pub segment_build_superbatch_max_secs: u64,
    #[env_config(
        name = "ZO_VIX_MAX_RAW_TERM_LENGTH",
        default = 65532,
        help = "Raw (non-full-text) values longer than this are skipped from the term index \
                WITHOUT degrading the field (owner call 2026-08-12, performance-first — \
                previously one oversize value made the field partial for the whole file, \
                sending every query on it to the scan branch): the index stays authoritative, \
                so an equality search for a skipped oversize literal itself silently misses \
                those rows. Key terms still index the rows (IS [NOT] NULL exact); skips are \
                counted in writer stats. Full-text fields tokenize regardless of value length. \
                The 64KiB ceiling is a format bound (composite term key must fit the \
                dictionary key space) — do not raise it."
    )]
    pub vix_max_raw_term_len: usize,
    #[env_config(
        name = "ZO_VIX_DOCS_CHUNK_BYTES",
        default = 16777216,
        help = "Uncompressed-byte budget of one docs-blob chunk in core .vix files — the \
                decompression unit of a matched-row point read. Rows per chunk are \
                clamp(budget / avg_present_row_bytes, 64, ZO_VIX_DOCS_CHUNK_MAX_ROWS), so \
                the effective chunk can exceed the budget for very wide rows. Default \
                16 MiB (owner call 2026-08-18 on the M8 chunk-size sweep, S2: merge wall \
                −25% / merge VmHWM −17%, storage-neutral, cost ~2x _source point-read \
                decode; 4 MiB remains the point-read-optimal knob value). 0 = the \
                16 MiB default."
    )]
    pub vix_docs_chunk_bytes: usize,
    #[env_config(
        name = "ZO_VIX_DOCS_CHUNK_MAX_ROWS",
        default = 65536,
        help = "Rows-per-chunk ceiling of the ZO_VIX_DOCS_CHUNK_BYTES clamp (the historical \
                hard cap, unchanged by the M9 budget default flip). The cap is \
                what bounds a huge byte budget: at ~1 KiB average rows a 64 MiB budget \
                already saturates it. Raising it toward the file's row count makes the whole \
                file one chunk = one zone-map/stats entry (no intra-file pruning), one \
                RowSelection granule and one DECOMPRESSION unit per matched-row point read \
                (M8 sweep knob). Values below the 64-row floor are raised to the floor; \
                0 = the 65,536 default."
    )]
    pub vix_docs_chunk_max_rows: usize,
    #[env_config(
        name = "ZO_VIX_STATS_MIN_DENSITY",
        default = 0.1,
        help = "H2 pay-as-you-go per-column chunk stats: presence-density threshold (present \
                rows / total rows) below which a docs column gets NO per-chunk min/max rows in \
                the data object's stats blob — it keeps its file-level presence count. Sparse \
                columns cost a presence entry, never a stats table. 0 = the built-in 0.1."
    )]
    pub vix_stats_min_density: f64,
    #[env_config(
        name = "ZO_VIX_STATS_MAX_BYTES",
        default = 1048576,
        help = "Byte cap of one data object's per-column chunk-stats blob (densest columns are \
                kept first; the rest keep file-level presence only). Bounds footer-region size \
                on wide corpora (H2: footer grows sub-linearly with column count). 0 = the \
                built-in 1 MiB."
    )]
    pub vix_stats_max_bytes: usize,
    #[env_config(
        name = "ZO_VIX_READ_MODE",
        default = "ranged",
        help = "How queries read .vix containers from object storage: 'ranged' (default) \
                fetches only the puffin footer, the term dictionary and the postings/docs \
                chunks a query touches via range GETs — cold index evaluation stops \
                downloading whole objects (whole-file background caching for the scan path \
                still applies); 'cached' downloads the whole object through the file cache \
                ladder before evaluating, as before."
    )]
    pub vix_read_mode: String,
    #[env_config(
        name = "ZO_VIX_MERGE_THREAD_NUM",
        default = 0,
        help = "Threads used by one core-file compaction merge (input decode + the \
                range-partitioned term-dictionary merge). 0 = the machine's available \
                parallelism. Each compact worker merge spawns its own set, so lower this \
                when many ZO_FILE_MERGE_THREAD_NUM workers merge concurrently."
    )]
    pub vix_merge_thread_num: usize,
    #[env_config(
        name = "ZO_VIX_MERGE_KWAY_THREADS",
        default = 0,
        help = "#51b: range parallelism of the compaction term-dictionary k-way merge — \
                the output key space splits into real-key ranges merged concurrently. \
                0 = min(available parallelism, 8); 1 = exactly one range (the sequential \
                path). Always additionally capped by the per-merge thread budget \
                (ZO_VIX_MERGE_THREAD_NUM), so it stacks with it rather than widening it."
    )]
    pub vix_merge_kway_threads: usize,
    #[env_config(
        name = "ZO_VIX_MERGE_TYPE_POLICY",
        default = "legacy",
        help = "Type target and column-derivation policy for VIX compaction rebuilds. 'legacy' \
                keeps the current latest-schema target and requires derivation-equivalent input \
                types; 'latest_schema' keeps that target and additionally admits Boolean/integer/\
                finite-float to string-family casts. Reverse parsing and numeric narrowing remain \
                on the _source fallback. When such a cast is admitted over complete columns, the \
                opt-in mode rewrites both docs and _source to the authoritative string type so \
                later heals and legacy rollback reproduce identical terms. Start dark and canary \
                on compactor pods only."
    )]
    pub vix_merge_type_policy: String,
    #[env_config(
        name = "ZO_VIX_REBUILD_CONCURRENCY",
        default = 0,
        help = "M12: process-wide admission for REBUILD-path compaction merges (decode + \
                re-derive terms from _source — the memory-heavy shape; the dev launch's \
                first-hour OOM wave was 8 concurrent first-gen rebuilds over multi-GB \
                groups). At most this many rebuilds run at once; extra rebuild-path \
                merges WAIT (their worker blocks) while passthrough/k-way fast-path \
                merges stay unthrottled. 0 = auto: max(1, ZO_FILE_MERGE_THREAD_NUM / 2). \
                Always admits at least one."
    )]
    pub vix_rebuild_concurrency: usize,
    #[env_config(
        name = "ZO_VIX_REBUILD_HEADROOM_MB",
        default = 5120,
        help = "M30: live-memory admission for rebuild slots BEYOND the guaranteed \
                first one. An extra rebuild is admitted only while sampled process RSS \
                plus this many MB charged for every extra rebuild in flight (the \
                candidate included) stays under 90% of the memory limit — \
                ZO_VIX_REBUILD_CONCURRENCY becomes the hard CAP and live RSS decides \
                how much of it is usable moment to moment. This replaces the blind \
                count raise the M12 gate contract owed: per-rebuild transit varies \
                5-10x per stream, so it is bounded at runtime instead of estimated. \
                Waiters re-check every 500ms as RSS moves. 0 = no memory check (the \
                count is the only gate, exact M12 behavior). The first rebuild always \
                admits regardless, so progress is guaranteed."
    )]
    pub vix_rebuild_headroom_mb: usize,
    #[env_config(
        name = "ZO_VIX_MERGE_INDEX_DEFER_BELOW_MB",
        default = 0,
        help = "M31: defer the inverted-index build on NON-FINAL compaction merges. A \
                core-file merge group whose inputs are ALL index-less (index_size 0: \
                L0s and previously deferred outputs) and whose summed original_size is \
                below this many MB writes a COLUMN-STORE-ONLY output (no dictionary, \
                no postings, no bloom, no rebuild-gate admission — the copy-shape \
                merge), because an output below the ZO_COMPACT_MAX_FILE_SIZE/2 debt \
                line is guaranteed to be merged again — building its index would be \
                thrown away on the next hop. The index is built exactly once, when a \
                group crosses this line (or by the existing single-file heal when a \
                deferred leftover is the partition's terminal file). Sane value: \
                ZO_COMPACT_MAX_FILE_SIZE/2. 0 = off (today's behavior: every merge \
                output is indexed). Interim cost: a deferred output is queryable \
                exactly like an L0 (column scans, no term index) until its final hop."
    )]
    pub vix_merge_index_defer_below_mb: usize,
    #[env_config(
        name = "ZO_VIX_BUILD_THREAD_NUM",
        default = 0,
        help = "Threads used to ENCODE one single-file core build on the WAL→storage \
                move job (the `docs` + index blob compression pipeline). 0 = auto: the \
                machine's available parallelism on a DEDICATED ingester (spare cores, no \
                query/compaction competition), else 1 (a combined ingester+querier/compactor \
                node keeps the build single-threaded so it never competes with the query \
                fan-out or compaction merge — the freed cores serve query tail latency). \
                The move job already builds separate files in parallel across \
                ZO_FILE_MOVE_THREAD_NUM workers; raise this only when files are few and \
                large so the across-file pool is underused. Term accumulation stays \
                single-core per file (measured non-dominant vs the across-file pool; see \
                WORKLOG PHASE C2)."
    )]
    pub vix_build_thread_num: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_RESULT_CACHE_ENABLED",
        default = true,
        help = "Toggle the vix per-file index result cache. Safe by construction: keys are \
                structural (condition hash + optimize-rule params + file key), files are \
                immutable, and only files fully inside the query range are served."
    )]
    pub inverted_index_result_cache_enabled: bool,
    #[env_config(
        name = "ZO_INVERTED_INDEX_COUNT_OPTIMIZER_ENABLED",
        default = true,
        help = "Toggle inverted index count optimizer."
    )]
    pub inverted_index_count_optimizer_enabled: bool,
    #[env_config(
        name = "ZO_QUERY_ON_STREAM_SELECTION",
        default = true,
        help = "Toggle search to be trigger based on button click event."
    )]
    pub query_on_stream_selection: bool,
    #[env_config(
        name = "ZO_SHOW_STREAM_DATES_DOCS_NUM",
        default = true,
        help = "Show docs count and stream dates"
    )]
    pub show_stream_dates_doc_num: bool,
    #[env_config(name = "ZO_INGEST_BLOCKED_STREAMS", default = "")] // use comma to split
    pub blocked_streams: String,
    #[env_config(name = "ZO_REPORT_USER_NAME", default = "")]
    pub report_user_name: String,
    #[env_config(name = "ZO_REPORT_USER_PASSWORD", default = "")]
    pub report_user_password: String,
    #[env_config(name = "ZO_REPORT_SERVER_URL", default = "http://localhost:5082")]
    pub report_server_url: String,
    #[env_config(name = "ZO_REPORT_SERVER_SKIP_TLS_VERIFY", default = false)]
    pub report_server_skip_tls_verify: bool,
    #[env_config(name = "ZO_SKIP_FORMAT_STREAM_NAME", default = false)]
    pub skip_formatting_stream_name: bool,
    #[env_config(name = "ZO_FORMAT_STREAM_NAME_TO_LOWERCASE", default = true)]
    pub format_stream_name_to_lower: bool,
    #[env_config(name = "ZO_BULK_RESPONSE_INCLUDE_ERRORS_ONLY", default = false)]
    pub bulk_api_response_errors_only: bool,
    #[env_config(
        name = "ZO_MEM_TABLE_STREAMS",
        default = "",
        help = "Streams for which dedicated MemTable will be used as comma separated values"
    )]
    pub mem_table_individual_streams: String,
    #[env_config(
        name = "ZO_SELF_METRIC_CONSUMPTION_ENABLED",
        default = false,
        help = "self-consume metrics generated by openobserve"
    )]
    pub self_metrics_consumption_enabled: bool,
    #[env_config(
        name = "ZO_SELF_METRIC_CONSUMPTION_INTERVAL",
        default = 60,
        help = "metrics self-consumption interval, unit seconds"
    )]
    pub self_metrics_consumption_interval: u64,
    #[env_config(
        name = "ZO_SELF_METRIC_CONSUMPTION_ACCEPTLIST",
        default = "",
        help = "only these metrics will be self-consumed, comma separated"
    )]
    pub self_metrics_consumption_whitelist: String,
    #[env_config(
        name = "ZO_RESULT_CACHE_ENABLED",
        default = true,
        help = "Enable result cache for query results"
    )]
    pub result_cache_enabled: bool,
    #[env_config(
        name = "ZO_USE_MULTIPLE_RESULT_CACHE",
        default = false,
        help = "Enable to use mulple result caches for query results"
    )]
    pub use_multi_result_cache: bool,
    #[env_config(
        name = "ZO_RESULT_CACHE_SELECTION_STRATEGY",
        default = "overlap",
        help = "Strategy to use for result cache, default is both, possible value - both, overlap, duration"
    )]
    pub result_cache_selection_strategy: String,
    #[env_config(name = "ZO_SWAGGER_ENABLED", default = true)]
    pub swagger_enabled: bool,
    #[env_config(
        name = "ZO_REGEX_PATTERNS_SOURCE_URL",
        default = "https://raw.githubusercontent.com/openobserve/sdr_patterns/main/regex.json",
        help = "URL for built-in regex patterns JSON source. Can be customized to use different pattern libraries."
    )]
    pub regex_patterns_source_url: String,
    #[env_config(
        name = "ZO_MODEL_PRICING_ENABLED",
        default = true,
        help = "Enable user-defined model pricing. When true, uses DB pricing definitions and syncs from GitHub. When false, falls back to hardcoded built-in pricing only."
    )]
    pub model_pricing_enabled: bool,
    #[env_config(
        name = "ZO_MODEL_PRICING_SOURCE_URL",
        default = "https://raw.githubusercontent.com/openobserve/sdr_patterns/refs/heads/main/llm_pricing.json",
        help = "URL for built-in LLM model pricing JSON source."
    )]
    pub model_pricing_source_url: String,
    #[env_config(
        name = "ZO_MODEL_PRICING_SYNC_INTERVAL_SECS",
        default = 21600,
        help = "Interval in seconds for syncing built-in model pricing from GitHub. Default: 6 hours (21600)."
    )]
    pub model_pricing_sync_interval_secs: u64,
    #[env_config(name = "ZO_FAKE_ES_VERSION", default = "")]
    pub fake_es_version: String,
    #[env_config(name = "ZO_ES_VERSION", default = "")]
    pub es_version: String,
    #[env_config(
        name = "ZO_CREATE_ORG_THROUGH_INGESTION",
        default = true,
        help = "If true (default true), new org can be automatically created through ingestion for root user. This can be changed in the runtime."
    )]
    pub create_org_through_ingestion: bool,
    #[env_config(
        name = "ZO_ORG_INVITE_EXPIRY",
        default = 7,
        help = "The number of days (default 7) an invitation token will be valid for. This can be changed in the runtime."
    )]
    pub org_invite_expiry: u32,
    #[env_config(
        name = "ZO_MIN_AUTO_REFRESH_INTERVAL",
        default = 5,
        help = "allow minimum auto refresh interval in seconds"
    )] // in seconds
    pub min_auto_refresh_interval: u32,
    #[env_config(name = "ZO_ADDITIONAL_REPORTING_ORGS", default = "")]
    pub additional_reporting_orgs: String,
    #[env_config(
        name = "ZO_USAGE_REPORT_TO_OWN_ORG",
        default = true,
        help = "Report alert/report triggers to the originating organization in addition to _meta org"
    )]
    pub usage_report_to_own_org: bool,
    #[env_config(
        name = "ZO_USE_STREAM_SETTINGS_FOR_PARTITIONS_ENABLED",
        default = false,
        help = "Enable to use stream settings for partitions. This will apply for all streams"
    )]
    pub use_stream_settings_for_partitions_enabled: bool,
    #[env_config(name = "ZO_DASHBOARD_PLACEHOLDER", default = "_o2_all_")]
    pub dashboard_placeholder: String,
    #[env_config(name = "ZO_AGGREGATION_TOPK_ENABLED", default = true)]
    pub aggregation_topk_enabled: bool,
    #[env_config(name = "ZO_SEARCH_INSPECTOR_ENABLED", default = false)]
    pub search_inspector_enabled: bool,
    #[env_config(name = "ZO_UTF8_VIEW_ENABLED", default = true)]
    pub utf8_view_enabled: bool,
    #[env_config(
        name = "ZO_DASHBOARD_SHOW_SYMBOL_ENABLED",
        default = false,
        help = "Enable to show symbol in dashboard"
    )]
    pub dashboard_show_symbol_enabled: bool,
    #[env_config(
        name = "ZO_DASHBOARD_SHOW_FIELD_AS_JSON_ENABLED",
        default = false,
        help = "Enable to show field as JSON in dashboard table"
    )]
    pub dashboard_show_field_as_json_enabled: bool,
    #[env_config(name = "ZO_INGEST_DEFAULT_HEC_STREAM", default = "")]
    pub default_hec_stream: String,
    #[env_config(
        name = "ZO_CONFIG_WATCHER_INTERVAL",
        default = 30,
        help = "Config file watcher interval in seconds. Set to 0 to disable"
    )]
    pub env_watcher_interval: u64,
    #[env_config(
        name = "ZO_LOG_PAGE_DEFAULT_FIELD_LIST",
        default = "all",
        help = "Which fields to show by default in logs search page. Valid values - all,interesting"
    )]
    pub log_page_default_field_list: String,
    #[env_config(
        name = "ZO_TRACES_LIST_INDEX_ENABLED",
        default = true,
        help = "enable trace list index for traces"
    )]
    pub traces_list_index_enabled: bool,
    #[env_config(
        name = "ZO_INGESTION_LOG_ENABLED",
        default = true,
        help = "enable ingestion error logs reporting"
    )]
    pub ingestion_log_enabled: bool,
    #[env_config(
        name = "ZO_ENABLE_CROSS_LINKING",
        default = false,
        help = "Enable cross-linking feature for drill-down links on log/trace records"
    )]
    pub enable_cross_linking: bool,
    #[env_config(
        name = "ZO_AUTO_QUERY_ENABLED",
        default = false,
        help = "Enable Live Mode feature in the UI. When true, users can toggle auto-query on filter/time-range changes. When false, the Live Mode toggle is hidden and Run Query button is always shown."
    )]
    pub auto_query_enabled: bool,
}

impl Common {
    pub fn should_create_span(&self) -> bool {
        self.tracing_enabled || self.tracing_search_enabled || self.search_inspector_enabled
    }

    /// Decoded-byte cap for one segment-builder `(stream, hour)` chunk.
    ///
    /// Compute this directly from the normalized config on every use: config
    /// is already process-global, so a second cache would make tests and
    /// runtime overrides observe different values. Saturation keeps an
    /// extreme environment override from wrapping to a tiny admission cap.
    pub fn segment_build_chunk_bytes(&self) -> usize {
        self.segment_build_chunk_mb
            .max(1)
            .saturating_mul(1024 * 1024)
    }
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Limit {
    // no need set by environment
    pub cpu_num: usize,
    pub real_cpu_num: usize,
    pub mem_total: usize,
    pub disk_total: usize,
    pub disk_free: usize,
    #[env_config(name = "ZO_PAYLOAD_LIMIT", default = 209715200)]
    pub req_payload_limit: usize,
    #[env_config(name = "ZO_MAX_FILE_RETENTION_TIME", default = 600)] // seconds
    pub max_file_retention_time: u64,
    // MB, per log file size limit on disk
    #[env_config(name = "ZO_MAX_FILE_SIZE_ON_DISK", default = 512)]
    pub max_file_size_on_disk: usize,
    // MB, per data file size limit in memory
    #[env_config(name = "ZO_MAX_FILE_SIZE_IN_MEMORY", default = 512)]
    pub max_file_size_in_memory: usize,
    // MB, total data size of memtable in memory
    #[env_config(name = "ZO_MEM_TABLE_MAX_SIZE", default = 0)]
    pub mem_table_max_size: usize,
    #[env_config(
        name = "ZO_MEM_TABLE_BUCKET_NUM",
        default = 0,
        help = "MemTable bucket num, default is 1"
    )] // default is 1
    pub mem_table_bucket_num: usize,
    #[env_config(
        name = "ZO_SEGMENT_SCAN_MAX_BYTES",
        default = 536870912,
        help = "Soft per-query budget (bytes) on not-yet-sealed live data one segment scan may keep; crossing it logs a warning and the query CONTINUES (0 disables the warning). The hard stop is half the pod's cgroup memory limit."
    )]
    pub segment_scan_max_bytes: usize,
    #[env_config(name = "ZO_MEM_PERSIST_INTERVAL", default = 2)] // seconds
    pub mem_persist_interval: u64,
    #[env_config(name = "ZO_WAL_WRITE_BUFFER_SIZE", default = 16384)] // 16 KB
    pub wal_write_buffer_size: usize,
    #[env_config(name = "ZO_WAL_WRITE_QUEUE_SIZE", default = 10000)] // 10k messages
    pub wal_write_queue_size: usize,
    #[env_config(name = "ZO_FILE_PUSH_INTERVAL", default = 2)] // seconds
    pub file_push_interval: u64,
    #[env_config(name = "ZO_FILE_PUSH_LIMIT", default = 0)] // files
    pub file_push_limit: usize,
    // over this limit will skip merging on ingester
    #[env_config(name = "ZO_FILE_MOVE_THREAD_NUM", default = 0)]
    pub file_move_thread_num: usize,
    #[env_config(
        name = "ZO_SHUTDOWN_MOVE_DEADLINE",
        default = 480,
        help = "Ingester shutdown: after the memtable flush, keep moving WAL parquet to \
                object storage for up to this many seconds so scale-in never strands data \
                on the released volume (OSS twin of the enterprise drain). 0 disables."
    )]
    pub shutdown_move_deadline: u64,
    #[env_config(name = "ZO_FILE_MERGE_THREAD_NUM", default = 0)]
    pub file_merge_thread_num: usize,
    #[env_config(name = "ZO_MEM_DUMP_THREAD_NUM", default = 0)]
    pub mem_dump_thread_num: usize,
    #[env_config(name = "ZO_VORTEX_THREAD_NUM", default = 0)]
    pub vortex_thread_num: usize,
    #[env_config(name = "ZO_USAGE_REPORTING_THREAD_NUM", default = 0)]
    pub usage_reporting_thread_num: usize,
    #[env_config(name = "ZO_QUERY_THREAD_NUM", default = 0)]
    pub query_thread_num: usize,
    #[env_config(
        name = "ZO_QUERY_MAX_CONCURRENCY",
        default = 30,
        help = "Max concurrent queries this node LEADS (SQL + promql). Requests past the limit get HTTP 429 immediately — no queueing (0 = unlimited). Since .80: replaces the global cluster query queue."
    )]
    pub query_max_concurrency: usize,
    #[env_config(
        name = "ZO_VIX_EAGER_TAIL_BYTES",
        default = 262144,
        help = "Eager tail fetch size for ranged .vix opens (bytes; 0 = built-in 64KiB). Index blobs cluster at the file's end, so a tail covering them turns a cold open + term eval into one ranged GET on small files."
    )]
    pub vix_eager_tail_bytes: u64,
    #[env_config(
        name = "ZO_WARMUP_CACHE_HOURS",
        default = 0,
        help = "Queriers prefetch the .vix index metadata (footer + directory tail) for THEIR consistent-hash share of the last N hours' files right after coming online, so post-roll cold starts serve index queries warm (0 = off). Best-effort background task; never blocks readiness."
    )]
    pub warmup_cache_hours: usize,
    #[env_config(
        name = "ZO_WARMUP_CACHE_MAX_FILES",
        default = 50000,
        help = "Upper bound on files one warmup pass opens (newest first)."
    )]
    pub warmup_cache_max_files: usize,
    #[env_config(
        name = "ZO_WARMUP_CACHE_CONCURRENCY",
        default = 4,
        help = "Concurrent index-metadata prefetches during warmup."
    )]
    pub warmup_cache_concurrency: usize,
    #[env_config(
        name = "ZO_QUERY_HTTP_HEARTBEAT_SECS",
        default = 5,
        help = "Oneshot /_search responses still running after this many seconds switch to a streamed body emitting a whitespace heartbeat every 2s, so a vanished HTTP/1.1 client fails the write and the query cancels server-side. Within the grace period status codes stay exact; after it, errors arrive in-body on a 200 (the code field). 0 disables."
    )]
    pub query_http_heartbeat_secs: u64,
    #[env_config(
        name = "ZO_VIX_SEARCH_CONCURRENCY",
        default = 0,
        help = "Number of .vix index files evaluated in parallel per query. Per-file work is \
                dominated by small IO waits, so this may exceed the core count. 0 = 4x CPU \
                cores, capped at 64."
    )]
    pub vix_search_concurrency: usize,
    #[env_config(
        name = "ZO_VIX_FETCH_TIMEOUT",
        default = 30, // seconds
        help = "Timeout in seconds for one .vix range fetch (footer/dictionary/postings/docs \
                chunk). A hung object-store connection becomes an error the per-file \
                retry/degradation path handles instead of stalling the query. 0 disables the \
                timeout."
    )]
    pub vix_fetch_timeout: u64,
    #[env_config(name = "ZO_FILE_DOWNLOAD_THREAD_NUM", default = 0)]
    pub file_download_thread_num: usize,
    #[env_config(name = "ZO_FILE_DOWNLOAD_MIN_RECORDS", default = 100)]
    pub file_download_min_records: i64,
    #[env_config(name = "ZO_FILE_DOWNLOAD_PRIORITY_QUEUE_THREAD_NUM", default = 0)]
    pub file_download_priority_queue_thread_num: usize,
    #[env_config(name = "ZO_FILE_DOWNLOAD_PRIORITY_QUEUE_WINDOW_SECS", default = 3600)]
    pub file_download_priority_queue_window_secs: i64,
    #[env_config(name = "ZO_FILE_DOWNLOAD_ENABLE_PRIORITY_QUEUE", default = true)]
    pub file_download_enable_priority_queue: bool,
    #[env_config(name = "ZO_GRPC_INGEST_TIMEOUT", default = 600)]
    pub grpc_ingest_timeout: u64,
    #[env_config(name = "ZO_QUERY_TIMEOUT", default = 600)]
    pub query_timeout: u64,
    #[env_config(
        name = "ZO_QUERY_INGESTER_TIMEOUT",
        default = 0,
        help = "Timeout for ingester query, default equal to query_timeout"
    )]
    pub query_ingester_timeout: u64,
    #[env_config(
        name = "ZO_QUERY_QUERIER_TIMEOUT",
        default = 0,
        help = "Timeout for querier query, default equal to query_timeout"
    )]
    pub query_querier_timeout: u64,
    #[env_config(name = "ZO_QUERY_DEFAULT_LIMIT", default = 1000)]
    pub query_default_limit: i64,
    #[env_config(name = "ZO_QUERY_VALUES_DEFAULT_NUM", default = 10)]
    pub query_values_default_num: i64,
    #[env_config(name = "ZO_QUERY_GROUP_BASE_SPEED", default = 1024)] // MB/s/core
    pub query_group_base_speed: usize,
    #[env_config(name = "ZO_QUERY_PARTITION_BY_SECS", default = 5)] // seconds
    pub query_partition_by_secs: usize,
    #[env_config(name = "ZO_QUERY_PARTITION_MAX_NUM", default = 100)] // max number of partitions
    pub query_partition_max_num: usize,
    #[env_config(name = "ZO_DISABLE_PARTITIONS_FOR_NON_TS_ORDER_BY", default = false)]
    pub disable_partitions_for_non_ts_order_by: bool,
    // Default Config: Run Query Recommendation Analysis for last one hour for every hour
    #[env_config(name = "ZO_QUERY_RECOMMENDATION_DURATION", default = 3600000000)] // microseconds
    pub query_recommendation_duration: i64,
    #[env_config(name = "ZO_QUERY_RECOMMENDATION_INTERVAL", default = 3600)] // seconds
    pub query_recommendation_analysis_interval: i64,
    #[env_config(name = "ZO_QUERY_RECOMMENDATION_TOP_K", default = 128)]
    pub query_recommendation_top_k: usize,
    #[env_config(name = "ZO_INGEST_ALLOWED_UPTO", default = 5)] // in hours - in past
    pub ingest_allowed_upto: i64,
    pub ingest_allowed_upto_micro: i64,
    #[env_config(name = "ZO_INGEST_ALLOWED_IN_FUTURE", default = 24)] // in hours - in future
    pub ingest_allowed_in_future: i64,
    pub ingest_allowed_in_future_micro: i64,
    #[env_config(name = "ZO_INGEST_FLATTEN_LEVEL", default = 3)] // default flatten level
    pub ingest_flatten_level: u32,
    // Deprecated: use ZO_LOGS_QUERY_RETENTION instead. Will be removed in a future version.
    #[env_config(name = "ZO_LOGS_FILE_RETENTION", default = "hourly")]
    pub logs_file_retention: String,
    // Deprecated: use ZO_TRACES_QUERY_RETENTION instead. Will be removed in a future version.
    #[env_config(name = "ZO_TRACES_FILE_RETENTION", default = "hourly")]
    pub traces_file_retention: String,
    // Deprecated: use ZO_METRICS_QUERY_RETENTION instead. Will be removed in a future version.
    #[env_config(name = "ZO_METRICS_FILE_RETENTION", default = "hourly")]
    pub metrics_file_retention: String,
    #[env_config(name = "ZO_LOGS_QUERY_RETENTION", default = "hourly")]
    pub logs_query_retention: String,
    #[env_config(name = "ZO_TRACES_QUERY_RETENTION", default = "hourly")]
    pub traces_query_retention: String,
    #[env_config(name = "ZO_METRICS_QUERY_RETENTION", default = "daily")]
    pub metrics_query_retention: String,
    #[env_config(name = "ZO_METRICS_LEADER_PUSH_INTERVAL", default = 15)]
    pub metrics_leader_push_interval: u64,
    #[env_config(name = "ZO_METRICS_LEADER_ELECTION_INTERVAL", default = 30)]
    pub metrics_leader_election_interval: i64,
    #[env_config(name = "ZO_METRICS_MAX_POINTS_PER_SERIES", default = 30000)]
    pub metrics_max_points_per_series: usize,
    #[env_config(name = "ZO_METRICS_MAX_SERIES_RESPONSE", default = 40000)]
    pub metrics_max_series_response: usize,
    #[env_config(name = "ZO_METRICS_CACHE_MAX_ENTRIES", default = 10000)]
    pub metrics_cache_max_entries: usize,
    #[env_config(name = "ZO_METRICS_INLIST_FILTER_ENABLED", default = false)]
    pub metrics_inlist_filter_enabled: bool,
    // Default raised 1000 -> 65536 (owner call 2026-08-12): the old limit
    // silently discarded k8s audit records with >1000 flattened keys
    // (thousands/burst on eks_audit_log). Kept as a backstop against truly
    // pathological million-key records; per-record cost scales only with
    // PRESENT fields under narrow-schema WAL batches.
    #[env_config(name = "ZO_COLS_PER_RECORD_LIMIT", default = 65536)]
    pub req_cols_per_record_limit: usize,
    #[env_config(name = "ZO_NODE_HEARTBEAT_TTL", default = 30)] // seconds
    pub node_heartbeat_ttl: i64,
    #[env_config(name = "ZO_HTTP_WORKER_NUM", default = 0)]
    pub http_worker_num: usize, // equals to cpu_num if 0
    #[env_config(name = "ZO_HTTP_WORKER_MAX_BLOCKING", default = 0)]
    pub http_worker_max_blocking: usize, // equals to 256 if 0
    #[env_config(name = "ZO_GRPC_RUNTIME_WORKER_NUM", default = 0)]
    pub grpc_runtime_worker_num: usize, // equals to cpu_num if 0
    #[env_config(name = "ZO_GRPC_RUNTIME_BLOCKING_WORKER_NUM", default = 0)]
    pub grpc_runtime_blocking_worker_num: usize, // equals to 512 if 0
    #[env_config(name = "ZO_GRPC_RUNTIME_SHUTDOWN_TIMEOUT", default = 10)] // seconds
    pub grpc_runtime_shutdown_timeout: u64,
    #[env_config(name = "ZO_JOB_RUNTIME_WORKER_NUM", default = 0)]
    pub job_runtime_worker_num: usize, // equals to cpu_num if 0
    #[env_config(name = "ZO_JOB_RUNTIME_BLOCKING_WORKER_NUM", default = 0)]
    pub job_runtime_blocking_worker_num: usize, // equals to 512 if 0
    #[env_config(name = "ZO_JOB_RUNTIME_SHUTDOWN_TIMEOUT", default = 10)] // seconds
    pub job_runtime_shutdown_timeout: u64,
    #[env_config(name = "ZO_WAL_RUNTIME_WORKER_NUM", default = 0)]
    pub wal_runtime_worker_num: usize, // equals to mem_table_bucket_num if 0
    #[env_config(name = "ZO_CALCULATE_STATS_INTERVAL", default = 600)] // seconds
    pub calculate_stats_interval: u64,
    #[env_config(name = "ZO_HTTP_SHUTDOWN_TIMEOUT", default = 5)] // seconds
    pub http_shutdown_timeout: u64,
    #[env_config(name = "ZO_HTTP_SLOW_LOG_THRESHOLD", default = 5)] // seconds
    pub http_slow_log_threshold: u64,
    #[env_config(name = "ZO_ALERT_SCHEDULE_INTERVAL", default = 10)] // seconds
    pub alert_schedule_interval: i64,
    #[env_config(name = "ZO_ALERT_SCHEDULE_CONCURRENCY", default = 5)]
    pub alert_schedule_concurrency: i64,
    #[env_config(name = "ZO_ALERT_SCHEDULE_TIMEOUT", default = 90)] // seconds
    pub alert_schedule_timeout: i64,
    #[env_config(
        name = "ZO_ALERT_PREVIEW_TIMERANGE_MINUTES",
        default = 0,
        help = "Time range in minutes for alert preview. If set to 0 (default), uses the alert's period value. If greater than 0, overrides period for preview."
    )]
    pub alert_preview_timerange_minutes: i64,
    #[env_config(name = "ZO_REPORT_SCHEDULE_TIMEOUT", default = 300)] // seconds
    pub report_schedule_timeout: i64,
    #[env_config(name = "ZO_DERIVED_STREAM_SCHEDULE_INTERVAL", default = 300)] // seconds
    pub derived_stream_schedule_interval: i64,
    #[env_config(name = "ZO_SCHEDULER_MAX_RETRIES", default = 3)]
    pub scheduler_max_retries: i32,
    #[env_config(name = "ZO_SCHEDULER_PAUSE_ALERT_AFTER_RETRIES", default = false)]
    pub pause_alerts_on_retries: bool,
    #[env_config(
        name = "ZO_ALERT_CONSIDERABLE_DELAY",
        default = 20,
        help = "Integer value representing the delay in percentage of the alert frequency that will be included in alert evaluation timerange. Default is 20. This can be changed in runtime."
    )]
    pub alert_considerable_delay: i32,
    #[env_config(name = "ZO_SCHEDULER_WATCH_INTERVAL", default = 30)] // seconds
    pub scheduler_watch_interval: i64,
    // Per-module scheduler pullers (Part A / A3+A4). When enabled, each TriggerModule gets its
    // own pull loop, cadence, LIMIT budget, channel and worker pool, so a backlog or slow handler
    // in one module cannot starve another. Default off → single shared puller (legacy behavior).
    #[env_config(
        name = "ZO_SCHEDULER_PER_MODULE_PULLERS",
        default = false,
        help = "Run a dedicated pull loop + worker pool per scheduler module. When false, a single shared puller handles all modules (legacy)."
    )]
    pub scheduler_per_module_pullers: bool,
    // Per-module concurrency (LIMIT + channel cap + worker count). 0 = inherit
    // ZO_ALERT_SCHEDULE_CONCURRENCY. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true.
    // Backfill defaults to the smallest budget so bulk/background jobs never crowd out others.
    // Note: the alert lane reuses ZO_ALERT_SCHEDULE_CONCURRENCY directly (no duplicate var).
    #[env_config(
        name = "ZO_SCHEDULER_REPORT_CONCURRENCY",
        default = 0,
        help = "Max report jobs pulled per cycle and the report worker-pool size. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_CONCURRENCY."
    )]
    pub scheduler_report_concurrency: i64,
    #[env_config(
        name = "ZO_SCHEDULER_DERIVED_STREAM_CONCURRENCY",
        default = 0,
        help = "Max derived-stream/pipeline jobs pulled per cycle and the worker-pool size. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_CONCURRENCY."
    )]
    pub scheduler_derived_stream_concurrency: i64,
    #[env_config(
        name = "ZO_SCHEDULER_BACKFILL_CONCURRENCY",
        default = 1,
        help = "Max backfill jobs pulled per cycle and the backfill worker-pool size. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. Defaults to 1 (smallest budget) so bulk backfills never crowd out latency-sensitive modules."
    )]
    pub scheduler_backfill_concurrency: i64,
    #[env_config(
        name = "ZO_SCHEDULER_ANOMALY_CONCURRENCY",
        default = 0,
        help = "Max anomaly-detection jobs pulled per cycle and the worker-pool size. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_CONCURRENCY."
    )]
    pub scheduler_anomaly_concurrency: i64,
    #[env_config(
        name = "ZO_SCHEDULER_QUERY_RECO_CONCURRENCY",
        default = 0,
        help = "Max query-recommendation jobs pulled per cycle and the worker-pool size. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_CONCURRENCY."
    )]
    pub scheduler_query_reco_concurrency: i64,
    // Per-module poll cadence in seconds. 0 = inherit ZO_ALERT_SCHEDULE_INTERVAL (the alert pull
    // frequency). Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. One var per module so each
    // puller can poll at its own rate (e.g. backfill slower, synthetics faster). The alert lane
    // reuses ZO_ALERT_SCHEDULE_INTERVAL directly (no duplicate var).
    #[env_config(
        name = "ZO_SCHEDULER_REPORT_INTERVAL",
        default = 0, // seconds
        help = "Poll cadence in seconds for the report puller. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_INTERVAL."
    )]
    pub scheduler_report_interval: i64,
    #[env_config(
        name = "ZO_SCHEDULER_DERIVED_STREAM_INTERVAL",
        default = 0, // seconds
        help = "Poll cadence in seconds for the derived-stream/pipeline puller. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_INTERVAL."
    )]
    pub scheduler_derived_stream_interval: i64,
    #[env_config(
        name = "ZO_SCHEDULER_BACKFILL_INTERVAL",
        default = 0, // seconds
        help = "Poll cadence in seconds for the backfill puller. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_INTERVAL."
    )]
    pub scheduler_backfill_interval: i64,
    #[env_config(
        name = "ZO_SCHEDULER_ANOMALY_INTERVAL",
        default = 0, // seconds
        help = "Poll cadence in seconds for the anomaly-detection puller. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_INTERVAL."
    )]
    pub scheduler_anomaly_interval: i64,
    #[env_config(
        name = "ZO_SCHEDULER_QUERY_RECO_INTERVAL",
        default = 0, // seconds
        help = "Poll cadence in seconds for the query-recommendation puller. Only used when ZO_SCHEDULER_PER_MODULE_PULLERS=true. 0 inherits ZO_ALERT_SCHEDULE_INTERVAL."
    )]
    pub scheduler_query_reco_interval: i64,
    #[env_config(name = "ZO_SEARCH_JOB_WORKS", default = 1)]
    pub search_job_workers: i64,
    #[env_config(name = "ZO_SEARCH_JOB_SCHEDULE_INTERVAL", default = 10)] // seconds
    pub search_job_scheduler_interval: i64,
    #[env_config(
        name = "ZO_SEARCH_JOB_RUN_TIMEOUT",
        default = 600, // seconds
        help = "Timeout for update check"
    )]
    pub search_job_run_timeout: i64,
    #[env_config(name = "ZO_SEARCH_JOB_DELETE_INTERVAL", default = 600)] // seconds
    pub search_job_delete_interval: i64,
    #[env_config(
        name = "ZO_SEARCH_JOB_TIMEOUT",
        default = 36000, // seconds
        help = "Timeout for query"
    )]
    pub search_job_timeout: i64,
    #[env_config(
        name = "ZO_SEARCH_JOB_RETENTION",
        default = 30, // days
        help = "Retention for search job"
    )]
    pub search_job_retention: i64,
    #[env_config(name = "ZO_STARTING_EXPECT_QUERIER_NUM", default = 0)]
    pub starting_expect_querier_num: usize,
    #[env_config(name = "ZO_QUERY_OPTIMIZATION_NUM_FIELDS", default = 1000)]
    pub query_optimization_num_fields: usize,
    #[env_config(name = "ZO_QUICK_MODE_ENABLED", default = false)]
    pub quick_mode_enabled: bool,
    #[env_config(name = "ZO_QUICK_MODE_FORCE_ENABLED", default = true)]
    pub quick_mode_force_enabled: bool,
    #[env_config(name = "ZO_QUICK_MODE_NUM_FIELDS", default = 500)]
    pub quick_mode_num_fields: usize,
    #[env_config(name = "ZO_QUICK_MODE_STRATEGY", default = "")]
    pub quick_mode_strategy: String, // first, last, both
    #[env_config(name = "ZO_META_CONNECTION_POOL_MIN_SIZE", default = 0)] // number of connections
    pub sql_db_connections_min: u32,
    #[env_config(name = "ZO_META_CONNECTION_POOL_MAX_SIZE", default = 0)] // number of connections
    pub sql_db_connections_max: u32,
    #[env_config(
        name = "ZO_META_CONNECTION_POOL_ACQUIRE_TIMEOUT",
        default = 0,
        help = "Seconds, Maximum acquire timeout of individual connections."
    )]
    pub sql_db_connections_acquire_timeout: u64,
    #[env_config(
        name = "ZO_META_IDLE_IN_TXN_TIMEOUT_SECS",
        default = 120,
        help = "Seconds before postgres kills a session idling INSIDE a transaction \
                (app-owned guard set per connection; 0 disables). Sessions of killed \
                pods otherwise hold their locks until TCP timeout (~30 min) and wedge \
                every claimer fleet-wide."
    )]
    pub sql_db_idle_in_txn_timeout_secs: u64,
    #[env_config(
        name = "ZO_META_CONNECTION_POOL_IDLE_TIMEOUT",
        default = 0,
        help = "Seconds, Maximum idle timeout of individual connections."
    )]
    pub sql_db_connections_idle_timeout: u64,
    #[env_config(
        name = "ZO_META_CONNECTION_POOL_MAX_LIFETIME",
        default = 0,
        help = "Seconds, Maximum lifetime of individual connections."
    )]
    pub sql_db_connections_max_lifetime: u64,
    #[env_config(
        name = "ZO_META_TRANSACTION_RETRIES",
        default = 3,
        help = "max time of transaction will retry"
    )]
    pub meta_transaction_retries: usize,
    #[env_config(
        name = "ZO_META_TRANSACTION_LOCK_TIMEOUT",
        default = 600,
        help = "timeout of transaction lock"
    )] // seconds
    pub meta_transaction_lock_timeout: usize,
    #[env_config(name = "ZO_DISTINCT_VALUES_INTERVAL", default = 10)] // seconds
    pub distinct_values_interval: u64,
    #[env_config(name = "ZO_DISTINCT_VALUES_HOURLY", default = false)]
    pub distinct_values_hourly: bool,
    #[env_config(name = "ZO_CONSISTENT_HASH_VNODES", default = 1000)]
    pub consistent_hash_vnodes: usize,
    #[env_config(
        name = "ZO_DATAFUSION_FILE_STAT_CACHE_MAX_SIZE",
        default = 0, // MB, default is 5% of total memory
        help = "Maximum memory size in MB for the file stat cache. Higher values allow caching more file statistics but increase memory usage."
    )]
    pub datafusion_file_stat_cache_max_size: usize,
    #[env_config(
        name = "ZO_DATAFUSION_STREAMING_AGGS_CACHE_MAX_ENTRIES",
        default = 10000,
        help = "Maximum number of entries in the streaming aggs cache. Higher values increase memory usage but may improve query performance."
    )]
    pub datafusion_streaming_aggs_cache_max_entries: usize,
    #[env_config(name = "ZO_DATAFUSION_MIN_PARTITION_NUM", default = 2)]
    pub datafusion_min_partition_num: usize,
    #[env_config(
        name = "ZO_ENRICHMENT_TABLE_LIMIT",
        default = 256,
        help = "Maximum size of a single enrichment table in mb"
    )]
    pub enrichment_table_max_size: usize,
    #[env_config(name = "ZO_SHORT_URL_RETENTION_DAYS", default = 30)] // days
    pub short_url_retention_days: i64,
    #[env_config(
        name = "ZO_INVERTED_INDEX_RESULT_CACHE_MAX_ENTRIES",
        default = 1000000, // roaring entries are ~70B-500B; the byte budget (MAX_SIZE) is the real bound
        help = "Maximum number of entries in the inverted index result cache. Higher values increase memory usage but may improve query performance."
    )]
    pub inverted_index_result_cache_max_entries: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_RESULT_CACHE_MAX_ENTRY_SIZE",
        default = 524288, // bytes, 512KB: a RowIds bitmap for a 4M-row file fits
        help = "Maximum size of a single entry in the inverted index result cache. Higher values increase memory usage but may improve query performance."
    )]
    pub inverted_index_result_cache_max_entry_size: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_RESULT_CACHE_MAX_SIZE",
        default = 0, // MB; 0 = 256MB. Hard byte budget across all entries.
        help = "Maximum total memory in MB for the vix per-file result cache. Entries are \
                evicted oldest-first once the budget is exceeded, so the entry-count and \
                per-entry limits can be generous without risking unbounded memory."
    )]
    pub inverted_index_result_cache_max_size: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_FOOTER_CACHE_MAX_SIZE",
        default = 0, // MB, default is 5% of total memory
        help = "Maximum memory size in MB for the footer cache. Higher values allow caching more file footers but increase memory usage."
    )]
    pub inverted_index_footer_cache_max_size: usize,
    #[env_config(
        name = "ZO_VIX_READER_CACHE_MAX_SIZE",
        default = 0, // MB, default is 10% of total memory (no upper clamp)
        help = "Maximum memory size in MB for the cache of parsed .vix readers (footer + \
                properties + term-dictionary FSTs) on queriers. Unset (0) defaults to 10% of \
                total memory with NO upper clamp — hosts serving many files should raise it \
                further (dictionaries dominate; hot queries do zero dictionary IO only while \
                their readers fit). Falls back to ZO_INVERTED_INDEX_FOOTER_CACHE_MAX_SIZE when \
                that legacy knob is set explicitly and this one is not."
    )]
    pub vix_reader_cache_max_size: usize,
    #[env_config(
        name = "ZO_BLOOM_FOOTER_CACHE_MAX_SIZE",
        default = 0, // MB, default is 1% of total memory, clamped to [32, 256] MB
        help = "Maximum memory size in MB for the bloom-filter footer cache. The cache holds the suffix bytes of each `.bf` (footer + tail of body) so subsequent prune calls skip the suffix-range GET. `.bf` body bytes are not cached here — they go through the regular file_data cache."
    )]
    pub bloom_footer_cache_max_size: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_SKIP_THRESHOLD",
        default = 35,
        help = "If the inverted index returns row_id more than this threshold(%), it will skip the inverted index."
    )]
    pub inverted_index_skip_threshold: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_TOPN_MAX_GROUP_NUM",
        default = 1000,
        help = "For top-n group by queries, a file with up to N distinct groups returns all of them, making its contribution to the merged result exact. Files with more groups keep only the limit-derived top-k and the merged top-n becomes approximate; raise to trade speed for accuracy."
    )]
    pub inverted_index_topn_max_group_num: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_MIN_TOKEN_LENGTH",
        default = 2,
        help = "Minimum length of a token in the inverted index."
    )]
    pub inverted_index_min_token_length: usize,
    #[env_config(
        name = "ZO_INVERTED_INDEX_MAX_TOKEN_LENGTH",
        default = 64,
        help = "Maximum length of a token in the inverted index."
    )]
    pub inverted_index_max_token_length: usize,
    #[env_config(
        name = "ZO_DEFAULT_MAX_QUERY_RANGE_DAYS",
        default = 0,
        help = "unit: Days. Global default max query range for all streams. If set to a value > 0, this will be used as the default max query range. Can be overridden by stream settings."
    )]
    pub default_max_query_range_days: i64,
    #[env_config(
        name = "ZO_MAX_QUERY_RANGE_FOR_SA",
        default = 0,
        help = "unit: Hour. Optional env variable to add restriction for SA, if not set SA will use max_query_range stream setting. When set which ever is smaller value will apply to api calls"
    )]
    pub max_query_range_for_sa: i64,
    #[env_config(
        name = "ZO_MAX_DASHBOARD_SERIES",
        default = 100,
        help = "maximum series to display in charts"
    )]
    pub max_dashboard_series: usize,
    #[env_config(
        name = "ZO_SEARCH_MINI_PARTITION_DURATION_SECS",
        default = 60,
        help = "Duration of each mini search partition in seconds"
    )]
    pub search_mini_partition_duration_secs: u64,
    #[env_config(
        name = "ZO_HISTOGRAM_ENABLED",
        help = "Show histogram for logs page",
        default = true
    )]
    pub histogram_enabled: bool,
    #[env_config(
        name = "ZO_TIMECHART_ENABLED",
        help = "Show timechart tab on logs page",
        default = false
    )]
    pub timechart_enabled: bool,
    #[env_config(
        name = "ZO_HISTOGRAM_BREAKDOWN_FIELDS",
        help = "Comma-separated ordered list of stream fields used for stacked histogram breakdown. First match wins. Default: severity,log_level,level,status",
        default = "severity,log_level,level,status"
    )]
    pub histogram_breakdown_fields: String,
    #[env_config(name = "ZO_CACHE_DELAY_SECS", default = 300)] // seconds
    pub cache_delay_secs: i64,
    #[env_config(
        name = "ZO_AGGS_MIN_NUM_PARTITIONS_SECS",
        default = 3,
        help = "Aggregates approximate number of seconds for executing search"
    )]
    pub aggs_min_num_partition_secs: usize,
    #[env_config(
        name = "ZO_BATCH_SIZE",
        default = 0,
        help = "Default is 8192, Batch size for parquet read/write operations and datafusion execution. Range: [1024, 8192]. Should carefully set this value, default is enough for most cases."
    )]
    pub batch_size: usize,
    #[env_config(
        name = "ZO_WORKFLOW_ERROR_RETAIN_DURATION",
        default = 2592000,
        help = "Default is 30 days, how many days in past to retain the errored workflow input files"
    )]
    pub workflow_error_retention_secs: i64,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Compact {
    #[env_config(name = "ZO_COMPACT_ENABLED", default = true)]
    pub enabled: bool,
    #[env_config(
        name = "ZO_COMPACT_LEASE_GENERATION_ENABLED",
        default = false,
        help = "Enable generation-fenced compactor merge and dump job claims after every node in the fleet has been upgraded."
    )]
    pub lease_generation_enabled: bool,
    #[env_config(name = "ZO_COMPACT_INTERVAL", default = 10)] // seconds
    pub interval: u64,
    #[env_config(
        name = "ZO_COMPACT_JOB_NUM",
        default = 0,
        help = "Concurrent merge jobs per compactor node (scheduler slots). 0 = ZO_FILE_MERGE_THREAD_NUM."
    )]
    pub job_num: usize,
    #[env_config(
        name = "ZO_COMPACT_LIVE_JOB_NUM",
        default = 2,
        help = "Total reserved scheduler slots outside the FIFO backlog lane. Without ZO_COMPACT_RECENT_LOOKBACK_HOURS all slots claim hot jobs at offsets >= now - ZO_COMPACT_LIVE_LOOKBACK_HOURS. When the recent lane is enabled, capacity is split approximately in half and the hot lane keeps the extra odd slot. 0 disables both lanes."
    )]
    pub live_job_num: usize,
    #[env_config(
        name = "ZO_COMPACT_LIVE_WORKER_NUM",
        default = 2,
        help = "Dedicated merge workers shared by the hot and optional recent-history schedulers."
    )]
    pub live_worker_num: usize,
    #[env_config(
        name = "ZO_COMPACT_LIVE_LOOKBACK_HOURS",
        default = 2,
        help = "The live lane claims jobs with offsets within this many hours of now."
    )]
    pub live_lookback_hours: i64,
    #[env_config(
        name = "ZO_COMPACT_RECENT_LOOKBACK_HOURS",
        default = 0,
        help = "Split the existing live lane between hot jobs and FIFO recent-history jobs in [now - this value, now - ZO_COMPACT_LIVE_LOOKBACK_HOURS). Requires ZO_COMPACT_LIVE_JOB_NUM >= 2; shares ZO_COMPACT_LIVE_WORKER_NUM workers, so enabling it does not add worker or scheduler capacity. 0 disables the split."
    )]
    pub recent_lookback_hours: i64,
    #[env_config(
        name = "ZO_COMPACT_DATA_RETENTION_INTERVAL",
        default = 3600,
        help = "Interval in seconds for the data retention job, default is 3600. Retention works at day granularity, so it doesn't need to run at ZO_COMPACT_INTERVAL"
    )] // seconds
    pub data_retention_interval: u64,
    #[env_config(name = "ZO_COMPACT_OLD_DATA_INTERVAL", default = 3600)] // seconds
    pub old_data_interval: u64,
    #[env_config(
        name = "ZO_COMPACT_MERGE_DEBT_INTERVAL",
        default = 60,
        help = "M29: seconds between merge-debt sweeps. Each sweep re-enqueues a merge job \
                for EVERY closed hour in the retention window that still holds >= \
                ZO_COMPACT_OLD_DATA_MIN_FILES small files (oldest hours first), so a \
                partially merged or late-filled hour is revisited within this interval \
                instead of waiting for the hourly old-data pass (and instead of the \
                ZO_COMPACT_OLD_DATA_MIN_HOURS dead zone stranding the newest closed \
                hours — the hot query window — entirely). Jobs dedup on (stream, hour): \
                a pending or running hour is never double-enqueued. 0 disables the lane."
    )]
    pub merge_debt_interval: u64,
    #[env_config(
        name = "ZO_COMPACT_BLOOM_BUILD_INTERVAL",
        default = 600,
        help = "Seconds between group .bf bloom-builder passes on the compactor (0 disables)."
    )]
    pub bloom_build_interval: u64,
    #[env_config(
        name = "ZO_COMPACT_BLOOM_BUILD_BATCH",
        default = 300,
        help = "Max pending (stream, hour) buckets one bloom-builder pass drains."
    )]
    pub bloom_build_batch: i64,
    #[env_config(
        name = "ZO_COMPACT_BLOOM_BUILD_FALLBACK_BUDGET",
        default = 128,
        help = "Max dictionary-stream backfills (blob-less old files) per builder pass; \
                leftovers stay queued and form later .bf chunks."
    )]
    pub bloom_build_fallback_budget: i64,
    #[env_config(name = "ZO_COMPACT_STRATEGY", default = "file_time")]
    // file_size, file_time, time_range
    pub strategy: String,
    #[env_config(name = "ZO_COMPACT_FAST_MODE", default = false)]
    pub fast_mode: bool,
    #[env_config(name = "ZO_COMPACT_SYNC_TO_DB_INTERVAL", default = 600)] // seconds
    pub sync_to_db_interval: u64,
    #[env_config(name = "ZO_COMPACT_MAX_FILE_SIZE", default = 2048)] // MB
    pub max_file_size: usize,
    #[env_config(
        name = "ZO_COMPACT_TRACES_INDEXED_MAX_FILE_SIZE",
        default = 0,
        help = "Max merged size in MB for indexed trace .vix inputs (index_size > 0). 0 \
                inherits ZO_COMPACT_MAX_FILE_SIZE; values below the global cap clamp to it. \
                Index-less trace rebuilds and every non-trace stream remain on the global cap."
    )]
    pub traces_indexed_max_file_size: usize,
    #[env_config(
        name = "ZO_COMPACT_DOWNLOAD_BUDGET_MB",
        default = 2048,
        help = "Process-wide cap (MB) on in-flight compaction download bytes across ALL merge \
                jobs: a download admits when its file's compressed_size fits the remaining \
                budget (a worker holding nothing always admits one, so oversize files cannot \
                starve). 0 = unlimited. The per-job concurrency semaphore still caps \
                parallelism; this caps bytes (H3, 2026-08-17 compactor OOM)."
    )]
    pub download_budget_mb: usize,
    #[env_config(name = "ZO_COMPACT_EXTENDED_DATA_RETENTION_DAYS", default = 3650)] // days
    pub extended_data_retention_days: i64,
    #[env_config(name = "ZO_COMPACT_OLD_DATA_STREAMS", default = "")] // use comma to split
    pub old_data_streams: String,
    #[env_config(name = "ZO_COMPACT_DATA_RETENTION_DAYS", default = 3650)] // days
    pub data_retention_days: i64,
    #[env_config(name = "ZO_COMPACT_OLD_DATA_MAX_DAYS", default = 7)] // days
    pub old_data_max_days: i64,
    #[env_config(name = "ZO_COMPACT_OLD_DATA_MIN_HOURS", default = 2)] // hours
    pub old_data_min_hours: i64,
    #[env_config(name = "ZO_COMPACT_OLD_DATA_MIN_FILES", default = 10)] // files
    pub old_data_min_files: i64,
    #[env_config(name = "ZO_COMPACT_DELETE_FILES_DELAY_HOURS", default = 2)] // hours
    pub delete_files_delay_hours: i64,
    #[env_config(name = "ZO_COMPACT_BLOCKED_ORGS", default = "")] // use comma to split
    pub blocked_orgs: String,
    #[env_config(name = "ZO_COMPACT_FILE_LIST_DELETED_MODE", default = "deleted")]
    pub file_list_deleted_mode: String, // "history" "deleted" "none"
    #[env_config(
        name = "ZO_COMPACT_FILE_LIST_DELETED_BATCH_SIZE",
        default = 1000,
        help = "batch size of file list deleted query"
    )]
    pub file_list_deleted_batch_size: usize,
    #[env_config(
        name = "ZO_COMPACT_FILE_LIST_MULTI_THREAD",
        default = false,
        help = "use multi thread for file list query"
    )]
    pub file_list_multi_thread: bool,
    #[env_config(name = "ZO_COMPACT_FILE_LIST_DUMP_ENABLED", default = false)]
    pub file_list_dump_enabled: bool,
    #[env_config(
        name = "ZO_COMPACT_BATCH_SIZE",
        default = 0,
        help = "Batch size for compact get pending jobs"
    )]
    pub batch_size: i64,
    #[env_config(
        name = "ZO_COMPACT_MAX_FILE_COUNT",
        default = 128,
        help = "Max INPUT FILES one merge batch takes (0 = bytes-only batching). The \
                byte budget alone let sliver-debt hours pack 1,600+ small files into \
                ONE k-way merge — memory scales with merge width (OOMKilled at 16Gi, \
                dev 2026-08-13) and heap CPU superlinearly. Oversized groups split \
                into multiple passes instead."
    )]
    pub max_file_count: i64,
    #[env_config(
        name = "ZO_COMPACT_JOB_RUN_TIMEOUT",
        default = 600, // 10 minutes
        help = "If a compact job is not finished in this time, it will be marked as failed"
    )]
    pub job_run_timeout: i64,
    #[env_config(
        name = "ZO_COMPACT_JOB_CLEAN_WAIT_TIME",
        default = 7200, // 2 hours
        help = "Clean the jobs which are finished more than this time"
    )]
    pub job_clean_wait_time: i64,
    #[env_config(name = "ZO_COMPACT_PENDING_JOBS_METRIC_INTERVAL", default = 300)] // seconds
    pub pending_jobs_metric_interval: u64,

    #[env_config(name = "ZO_COMPACT_MAX_GROUP_FILES", default = 10000)]
    pub max_group_files: usize,
    #[env_config(
        name = "ZO_COMPACT_RETENTION_ALLOWED_HOURS",
        default = "",
        help = "Comma-separated list of hours (0-23) when retention can run. Empty means run at all hours. Example: 5,6,8"
    )]
    pub retention_allowed_hours: String,
}
impl Compact {
    /// Merge byte ceiling for one homogeneous file class. Only indexed trace
    /// core files get the larger dictionary-passthrough target; index-less
    /// trace rebuilds and every other class stay on the global target.
    #[inline]
    pub fn max_file_size_for_merge(&self, stream_type: StreamType, indexed_core: bool) -> usize {
        if indexed_core
            && stream_type == StreamType::Traces
            && self.traces_indexed_max_file_size > 0
        {
            self.traces_indexed_max_file_size
        } else {
            self.max_file_size
        }
    }
}

#[derive(Serialize, EnvConfig, Default)]
pub struct CacheLatestFiles {
    // M11 default-on, owner call 2026-08-18: "cache_latest_files default to
    // true — we need cache latest files."
    #[env_config(
        name = "ZO_CACHE_LATEST_FILES_ENABLED",
        default = true,
        help = "Broadcast new file_list rows and cache the latest files on their consistent-hash querier; also forces the file_hash query partition strategy so queries land where the cache is. Default ON (owner call 2026-08-18)."
    )]
    pub enabled: bool,
    // cache data files: the `.vix` data object (or legacy parquet) AND its
    // `.vxi` index sidecar when the row's index_size > 0 (v2: index_size IS
    // the sidecar object's exact size, 0 = no sidecar)
    #[env_config(
        name = "ZO_CACHE_LATEST_FILES_PARQUET",
        default = true,
        help = "Cache the data object (.vix / legacy parquet) and, when the row's index_size > 0, its .vxi index sidecar."
    )]
    pub cache_parquet: bool,
    #[env_config(
        name = "ZO_CACHE_LATEST_FILES_DELETE_MERGE_FILES",
        default = true,
        help = "Evict merge inputs (data object + .vxi sidecar) from local caches when a merge broadcast replaces them. Default ON with the 2026-08-18 owner flip so caches drop inputs a merge superseded."
    )]
    pub delete_merge_files: bool,
    #[env_config(
        name = "ZO_CACHE_LATEST_FILES_DOWNLOAD_FROM_NODE",
        default = false,
        help = "Peer-to-peer cache fill from the broadcasting node. HELD BACK by the owner at the 2026-08-18 default flip: stays OFF at launch, queriers fill straight from object storage."
    )]
    pub download_from_node: bool,
    #[env_config(name = "ZO_CACHE_LATEST_FILES_DOWNLOAD_NODE_SIZE", default = 100)] // MB
    pub download_node_size: i64,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct MemoryCache {
    #[env_config(name = "ZO_MEMORY_CACHE_ENABLED", default = false)]
    pub enabled: bool,
    // Memory data cache strategy, default is lru, other value is fifo, time_lru
    #[env_config(name = "ZO_MEMORY_CACHE_STRATEGY", default = "lru")]
    pub cache_strategy: String,
    // Memory data cache bucket num, multiple bucket means multiple locker, default is 0
    #[env_config(name = "ZO_MEMORY_CACHE_BUCKET_NUM", default = 0)]
    pub bucket_num: usize,
    // MB, default is 50% of system memory
    #[env_config(name = "ZO_MEMORY_CACHE_MAX_SIZE", default = 0)]
    pub max_size: usize,
    // MB, will skip the cache when a query need cache great than this value, default is 50% of
    // max_size
    #[env_config(name = "ZO_MEMORY_CACHE_SKIP_SIZE", default = 0)]
    pub skip_size: usize,
    // MB, when cache is full will release how many data once time, default is 10% of max_size
    #[env_config(name = "ZO_MEMORY_CACHE_RELEASE_SIZE", default = 0)]
    pub release_size: usize,
    #[env_config(name = "ZO_MEMORY_CACHE_GC_SIZE", default = 100)] // MB
    pub gc_size: usize,
    #[env_config(name = "ZO_MEMORY_CACHE_GC_INTERVAL", default = 60)] // seconds
    pub gc_interval: u64,
    // Days, files with data older than this will not be downloaded into the cache,
    // queries read them directly from object storage. default 0 means no limit
    #[env_config(name = "ZO_MEMORY_CACHE_MAX_AGE_DAYS", default = 0)]
    pub max_age_days: i64,
    #[env_config(name = "ZO_MEMORY_CACHE_SKIP_DISK_CHECK", default = false)]
    pub skip_disk_check: bool,
    // MB, default is 50% of system memory
    #[env_config(name = "ZO_MEMORY_CACHE_DATAFUSION_MAX_SIZE", default = 0)]
    pub datafusion_max_size: usize,
    #[env_config(name = "ZO_MEMORY_CACHE_DATAFUSION_MEMORY_POOL", default = "")]
    pub datafusion_memory_pool: String,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct DiskCache {
    #[env_config(name = "ZO_DISK_CACHE_ENABLED", default = true)]
    pub enabled: bool,
    // Disk data cache strategy, default is lru, other value is fifo, time_lru
    #[env_config(name = "ZO_DISK_CACHE_STRATEGY", default = "time_lru")]
    pub cache_strategy: String,
    // Disk data cache bucket num, multiple bucket means multiple locker, default is 0
    #[env_config(name = "ZO_DISK_CACHE_BUCKET_NUM", default = 0)]
    pub bucket_num: usize,
    // MB, default is 50% of local volume available space and maximum 500GB
    #[env_config(name = "ZO_DISK_CACHE_MAX_SIZE", default = 0)]
    pub max_size: usize,
    // MB, default is 10% of local volume available space and maximum 20GB
    #[env_config(name = "ZO_DISK_RESULT_CACHE_MAX_SIZE", default = 0)]
    pub result_max_size: usize,
    #[env_config(name = "ZO_DISK_AGGREGATION_CACHE_MAX_SIZE", default = 0)]
    pub aggregation_max_size: usize,
    // MB, will skip the cache when a query need cache great than this value, default is 50% of
    // max_size
    #[env_config(name = "ZO_DISK_CACHE_SKIP_SIZE", default = 0)]
    pub skip_size: usize,
    // MB, when cache is full will release how many data once time, default is 10% of max_size
    #[env_config(name = "ZO_DISK_CACHE_RELEASE_SIZE", default = 0)]
    pub release_size: usize,
    #[env_config(name = "ZO_DISK_CACHE_GC_SIZE", default = 100)] // MB
    pub gc_size: usize,
    #[env_config(name = "ZO_DISK_CACHE_GC_INTERVAL", default = 60)] // seconds
    pub gc_interval: u64,
    // Days, files with data older than this will not be downloaded into the cache,
    // queries read them directly from object storage. default 0 means no limit
    #[env_config(name = "ZO_DISK_CACHE_MAX_AGE_DAYS", default = 0)]
    pub max_age_days: i64,
    #[env_config(name = "ZO_DISK_CACHE_MULTI_DIR", default = "")] // dir1,dir2,dir3...
    pub multi_dir: String,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct Log {
    #[env_config(name = "RUST_LOG", default = "info")]
    pub level: String,
    #[env_config(name = "ZO_LOG_JSON_FORMAT", default = false)]
    pub json_format: bool,
    #[env_config(name = "ZO_LOG_FILE_DIR", default = "")]
    pub file_dir: String,
    // default is: o2.{hostname}.log
    #[env_config(name = "ZO_LOG_FILE_NAME_PREFIX", default = "")]
    pub file_name_prefix: String,
    // logger timestamp local setup, eg: %Y-%m-%dT%H:%M:%SZ
    #[env_config(name = "ZO_LOG_LOCAL_TIME_FORMAT", default = "")]
    pub local_time_format: String,
}

#[derive(Serialize, Debug, EnvConfig, Default)]
pub struct Nats {
    #[env_config(name = "ZO_NATS_ADDR", default = "localhost:4222")]
    pub addr: String,
    #[env_config(name = "ZO_NATS_PREFIX", default = "o2_")]
    pub prefix: String,
    #[env_config(name = "ZO_NATS_USER", default = "")]
    pub user: String,
    #[env_config(name = "ZO_NATS_PASSWORD", default = "")]
    pub password: String,
    #[env_config(
        name = "ZO_NATS_REPLICAS",
        default = 3,
        help = "the copies of a given message to store in the NATS cluster.
        Can not be modified after bucket is initialized.
        To update this, delete and recreate the bucket."
    )]
    pub replicas: usize,
    #[env_config(
        name = "ZO_NATS_HISTORY",
        default = 1,
        help = "in the context of KV to configure how many historical entries to keep for a given bucket.
        Can not be modified after bucket is initialized.
        To update this, delete and recreate the bucket."
    )]
    pub history: i64,
    #[env_config(
        name = "ZO_NATS_DELIVER_POLICY",
        default = "all",
        help = "The point in the stream from which to receive messages, default is: all, valid option is: all, last, new."
    )]
    pub deliver_policy: String,
    #[env_config(name = "ZO_NATS_CONNECT_TIMEOUT", default = 5)]
    pub connect_timeout: u64,
    #[env_config(name = "ZO_NATS_COMMAND_TIMEOUT", default = 10)]
    pub command_timeout: u64,
    #[env_config(name = "ZO_NATS_LOCK_WAIT_TIMEOUT", default = 3600)]
    pub lock_wait_timeout: u64,
    #[env_config(name = "ZO_NATS_SUB_CAPACITY", default = 65535)]
    pub subscription_capacity: usize,
    #[env_config(name = "ZO_NATS_QUEUE_MAX_AGE", default = 60)] // days
    pub queue_max_age: u64,
    #[env_config(name = "ZO_NATS_EVENT_MAX_AGE", default = 3600)] // seconds
    pub event_max_age: u64,
    #[env_config(name = "ZO_NATS_LOCK_MAX_AGE", default = 7200)] // seconds
    pub lock_max_age: u64,
    #[env_config(
        name = "ZO_NATS_QUEUE_MAX_SIZE",
        help = "The maximum size of the queue in MB, default is 2048MB",
        default = 2048
    )]
    pub queue_max_size: i64,
    #[env_config(
        name = "ZO_NATS_EVENT_STORAGE",
        help = "Set the storage type for the event stream, default is: file, other value is: memory",
        default = "file"
    )]
    pub event_storage: String,
    #[env_config(
        name = "ZO_NATS_V211_SUPPORT",
        help = "Support NATS v2.11.x",
        default = false
    )]
    pub v211_support: bool,
    #[env_config(
        name = "ZO_NATS_KV_WATCH_MODULES",
        help = "Set the modules which need to use kv watcher",
        default = ""
    )]
    pub kv_watch_modules: String,
}

#[derive(Serialize, Debug, Default, EnvConfig)]
pub struct S3 {
    #[env_config(
        name = "ZO_S3_ACCOUNTS",
        default = "",
        help = "comma separated list of accounts"
    )]
    pub accounts: String,
    #[env_config(
        name = "ZO_S3_STREAM_STRATEGY",
        default = "",
        help = "stream strategy, default is: empty, only use default account, other value is: file_hash, stream_hash, stream1:account1,stream2:account2"
    )]
    pub stream_strategy: String,
    #[env_config(name = "ZO_S3_PROVIDER", default = "")]
    pub provider: String,
    #[env_config(name = "ZO_S3_SERVER_URL", default = "")]
    pub server_url: String,
    #[env_config(name = "ZO_S3_REGION_NAME", default = "")]
    pub region_name: String,
    #[env_config(name = "ZO_S3_ACCESS_KEY", default = "")]
    pub access_key: String,
    #[env_config(name = "ZO_S3_SECRET_KEY", default = "")]
    pub secret_key: String,
    #[env_config(name = "ZO_S3_BUCKET_NAME", default = "")]
    pub bucket_name: String,
    #[env_config(name = "ZO_S3_BUCKET_PREFIX", default = "")]
    pub bucket_prefix: String,
    #[env_config(name = "ZO_S3_CONNECT_TIMEOUT", default = 10)] // seconds
    pub connect_timeout: u64,
    #[env_config(
        name = "ZO_S3_REQUEST_TIMEOUT",
        default = 3600, // seconds
        help = "Object-store request timeout in seconds. The 3600s default suits large \
                uploads/downloads; on QUERIERS a stalled ranged read would otherwise pin a \
                query for the full hour — 60-120s is recommended there (the .vix range \
                fetches are additionally bounded by ZO_VIX_FETCH_TIMEOUT)."
    )]
    pub request_timeout: u64,
    #[env_config(name = "ZO_S3_FEATURE_FORCE_HOSTED_STYLE", default = false)]
    pub feature_force_hosted_style: bool,
    #[env_config(name = "ZO_S3_FEATURE_HTTP1_ONLY", default = false)]
    pub feature_http1_only: bool,
    #[env_config(name = "ZO_S3_FEATURE_HTTP2_ONLY", default = false)]
    pub feature_http2_only: bool,
    #[env_config(name = "ZO_S3_FEATURE_BULK_DELETE", default = false)]
    pub feature_bulk_delete: bool,
    #[env_config(name = "ZO_S3_ALLOW_INVALID_CERTIFICATES", default = false)]
    pub allow_invalid_certificates: bool,
    #[env_config(
        name = "ZO_S3_FEATURE_FORCE_INFREQUENT_ACCESS",
        default = false,
        help = "Use STANDARD_IA storage class for compliance storage type"
    )]
    pub feature_force_infrequent_access: bool,
    #[env_config(name = "ZO_S3_SYNC_TO_CACHE_INTERVAL", default = 600)] // seconds
    pub sync_to_cache_interval: u64,
    #[env_config(name = "ZO_S3_MAX_RETRIES", default = 10)]
    pub max_retries: usize,
    #[env_config(name = "ZO_S3_MAX_IDLE_PER_HOST", default = 0)]
    pub max_idle_per_host: usize,
    // https://github.com/hyperium/hyper/issues/2136#issuecomment-589488526
    #[env_config(name = "ZO_S3_CONNECTION_KEEPALIVE_TIMEOUT", default = 20)] // seconds
    pub keepalive_timeout: u64, // aws s3 by has timeout of 20 sec
    #[env_config(
        name = "ZO_S3_MULTI_PART_UPLOAD_SIZE",
        default = 100,
        help = "The size of the file will switch to multi-part upload in MB"
    )]
    pub multi_part_upload_size: usize,
}

#[derive(Serialize, Debug, EnvConfig, Default)]
pub struct Sns {
    #[env_config(name = "ZO_SNS_ENDPOINT", default = "")]
    pub endpoint: String,
    #[env_config(name = "ZO_SNS_CONNECT_TIMEOUT", default = 10)] // seconds
    pub connect_timeout: u64,
    #[env_config(name = "ZO_SNS_OPERATION_TIMEOUT", default = 30)] // seconds
    pub operation_timeout: u64,
}

#[derive(Serialize, Debug, EnvConfig, Default)]
pub struct Prometheus {
    #[env_config(name = "ZO_PROMETHEUS_HA_CLUSTER", default = "cluster")]
    pub ha_cluster_label: String,
    #[env_config(name = "ZO_PROMETHEUS_HA_REPLICA", default = "__replica__")]
    pub ha_replica_label: String,
    /// Max `le` labels (buckets + gap markers + inf) a native histogram sample may
    /// expand to; over-limit samples are downscaled (adjacent buckets merged).
    #[env_config(name = "ZO_PROMETHEUS_NATIVE_HISTOGRAM_MAX_BUCKETS", default = 16)]
    pub native_histogram_max_buckets: usize,
}

#[derive(Serialize, Debug, EnvConfig, Default)]
pub struct RUM {
    #[env_config(name = "ZO_RUM_ENABLED", default = false)]
    pub enabled: bool,
    #[env_config(name = "ZO_RUM_CLIENT_TOKEN", default = "")]
    pub client_token: String,
    #[env_config(name = "ZO_RUM_APPLICATION_ID", default = "")]
    pub application_id: String,
    #[env_config(name = "ZO_RUM_SITE", default = "")]
    pub site: String,
    #[env_config(name = "ZO_RUM_SERVICE", default = "")]
    pub service: String,
    #[env_config(name = "ZO_RUM_ENV", default = "")]
    pub env: String,
    #[env_config(name = "ZO_RUM_VERSION", default = "")]
    pub version: String,
    #[env_config(name = "ZO_RUM_ORGANIZATION_IDENTIFIER", default = "")]
    pub organization_identifier: String,
    #[env_config(name = "ZO_RUM_API_VERSION", default = "")]
    pub api_version: String,
    #[env_config(name = "ZO_RUM_INSECURE_HTTP", default = false)]
    pub insecure_http: bool,
}

#[derive(Serialize, Debug, EnvConfig, Default)]
pub struct Pipeline {
    #[env_config(
        name = "ZO_PIPELINE_REMOTE_STREAM_WAL_DIR",
        default = "",
        help = "For the remote stream WAL directory, if the pipeline destination is a remote stream, we use a separate path to distinguish between local WAL and remote WAL"
    )]
    pub remote_stream_wal_dir: String,
    #[env_config(
        name = "ZO_PIPELINE_REMOTE_STREAM_CONCURRENT_COUNT",
        default = 30,
        help = "control the remote stream wal send concurrent count"
    )]
    pub remote_stream_wal_concurrent_count: usize,
    #[env_config(
        name = "ZO_PIPELINE_OFFSET_FLUSH_INTERVAL",
        default = 10,
        help = "flush remote stream wal sended-ok-offset interval"
    )]
    pub offset_flush_interval: u64,
    #[env_config(
        name = "ZO_PIPELINE_REMOTE_REQUEST_TIMEOUT",
        default = 600,
        help = "pipeline exporter client request timeout"
    )]
    pub remote_request_timeout: u64,
    #[env_config(
        name = "ZO_PIPELINE_REMOTE_REQUEST_MAX_RETRY_TIME",
        default = 86400,
        help = "pipeline exporter client request max retry times, default 1440 minutes(24 hours)， unit is seconds"
    )]
    pub remote_request_max_retry_time: u64,
    #[env_config(
        name = "ZO_PIPELINE_WAL_SIZE_LIMIT",
        default = 0,
        help = "pipeline wal dir data size limit, default is 50% of local volume available space, unit is MB"
    )]
    pub wal_size_limit: u64,
    #[env_config(
        name = "ZO_PIPELINE_MAX_CONNECTIONS",
        default = 1024,
        help = "pipeline exporter client max connections"
    )]
    pub max_connections: usize,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_ENABLED",
        default = false,
        help = "Enable batching of entries before sending HTTP requests"
    )]
    pub batch_enabled: bool,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_SIZE",
        default = 100,
        help = "Maximum number of entries to batch together"
    )]
    pub batch_size: usize,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_TIMEOUT_MS",
        default = 1000,
        help = "Maximum time to wait for a batch to fill up (in milliseconds)"
    )]
    pub batch_timeout_ms: u64,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_SIZE_BYTES",
        default = 10485760, // 10MB
        help = "Maximum size of a batch in bytes"
    )]
    pub batch_size_bytes: usize,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_RETRY_MAX_ATTEMPTS",
        default = 3,
        help = "Maximum number of retries for batch flush"
    )]
    pub batch_retry_max_attempts: u32,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_RETRY_INITIAL_DELAY_MS",
        default = 1000, // 1 second
        help = "Initial delay for batch flush retry (in milliseconds)"
    )]
    pub batch_retry_initial_delay_ms: u64,
    #[env_config(
        name = "ZO_PIPELINE_BATCH_RETRY_MAX_DELAY_MS",
        default = 30000, // 30 seconds
        help = "Maximum delay for batch flush retry (in milliseconds)"
    )]
    pub batch_retry_max_delay_ms: u64,
    #[env_config(
        name = "ZO_PIPELINE_USE_SHARED_HTTP_CLIENT",
        default = false,
        help = "Use shared HTTP client instances for better connection pooling"
    )]
    pub use_shared_http_client: bool,
    #[env_config(
        name = "ZO_PIPELINE_REMOVE_FILE_AFTER_MAX_RETRY",
        default = true,
        help = "Remove wal file after max retry"
    )]
    pub remove_file_after_max_retry: bool,
    #[env_config(
        name = "ZO_PIPELINE_MAX_RETRY_COUNT",
        default = 10,
        help = "pipeline exporter client max retry count"
    )]
    pub max_retry_count: u32,
    #[env_config(
        name = "ZO_PIPELINE_MAX_RETRY_TIME_IN_HOURS",
        default = 24,
        help = "pipeline exporter client max retry time in hours"
    )]
    pub max_retry_time_in_hours: u64,
    #[env_config(
        name = "ZO_PIPELINE_MAX_FILE_SIZE_ON_DISK_MB",
        default = 256,
        help = "pipeline max file size on disk in MB"
    )]
    pub pipeline_max_file_size_on_disk_mb: usize,
    #[env_config(
        name = "ZO_PIPELINE_MAX_FILE_RETENTION_TIME_SECONDS",
        default = 600,
        help = "pipeline max file retention time in seconds"
    )]
    pub pipeline_max_file_retention_time_seconds: u64,
    #[env_config(
        name = "ZO_PIPELINE_FILE_PUSH_BACK_INTERVAL",
        default = 2,
        help = "duration in seconds to push the file to back to the queue after a read complete"
    )]
    pub pipeline_file_push_back_interval: u64,
    #[env_config(
        name = "ZO_PIPELINE_SINK_TASK_SPAWN_INTERVAL_MS",
        default = 100,
        help = "interval in milliseconds to spawn a new sink task"
    )]
    pub pipeline_sink_task_spawn_interval_ms: u64,
    #[env_config(
        name = "ZO_PIPELINE_ERROR_RETENTION_MINS",
        default = 60,
        help = "pipeline error retention time in minutes, errors older than this will be cleaned up"
    )]
    pub error_retention_mins: u64,
    #[env_config(
        name = "ZO_PIPELINE_ERROR_CLEANUP_INTERVAL",
        default = 300,
        help = "pipeline error cleanup interval in seconds"
    )]
    pub error_cleanup_interval: u64,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct HealthCheck {
    #[env_config(name = "ZO_HEALTH_CHECK_ENABLED", default = true)]
    pub enabled: bool,
    #[env_config(
        name = "ZO_HEALTH_CHECK_TIMEOUT",
        default = 5,
        help = "Health check timeout in seconds"
    )]
    pub timeout: u64,
    #[env_config(
        name = "ZO_HEALTH_CHECK_FAILED_TIMES",
        default = 3,
        help = "The node will be removed from consistent hash if health check failed exceed this times"
    )]
    pub failed_times: usize,
}

#[derive(Serialize, EnvConfig, Default)]
pub struct EnrichmentTable {
    #[env_config(
        name = "ZO_ENRICHMENT_TABLE_CACHE_DIR",
        default = "",
        help = "Local cache directory for enrichment tables"
    )]
    pub cache_dir: String,
    #[env_config(
        name = "ZO_ENRICHMENT_TABLE_MERGE_THRESHOLD_MB",
        default = 60,
        help = "Threshold for merging small files before S3 upload (in MB)"
    )]
    pub merge_threshold_mb: u64,
    #[env_config(
        name = "ZO_ENRICHMENT_TABLE_MERGE_INTERVAL",
        default = 600,
        help = "Background sync interval in seconds"
    )]
    pub merge_interval: u64,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_FETCH_MAX_SIZE",
        default = 500,
        help = "Maximum size of each batch when fetching from URL (in MB). Batches are saved to reduce database checkpoint frequency."
    )]
    pub url_fetch_max_size_mb: usize,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_FETCH_TIMEOUT",
        default = 7200,
        help = "Timeout for URL fetch operations (in seconds)"
    )]
    pub url_fetch_timeout_secs: u64,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_HEADER_FETCH_SIZE",
        default = 8192,
        help = "Size of initial fetch for CSV headers when resuming (in bytes). Should be large enough to contain the header row."
    )]
    pub url_header_fetch_size_bytes: usize,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_MAX_RETRIES",
        default = 3,
        help = "Maximum retry attempts for failed URL fetches"
    )]
    pub url_max_retries: u32,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_RETRY_DELAY",
        default = 5,
        help = "Delay between retry attempts (in seconds)"
    )]
    pub url_retry_delay_secs: u64,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_STALE_JOB_THRESHOLD",
        default = 600,
        help = "Jobs stuck in Processing status for longer than this are considered stale (in seconds). Used for automatic recovery."
    )]
    pub url_stale_job_threshold_secs: i64,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_RECOVERY_CHECK_INTERVAL",
        default = 120,
        help = "Interval between stale job recovery checks (in seconds). Each ingester will attempt to claim one stale job per interval."
    )]
    pub url_recovery_check_interval_secs: u64,
    #[env_config(
        name = "ZO_ENRICHMENT_URL_RECOVERY_JOBS_PER_CHECK",
        default = 1,
        help = "Number of stale jobs each ingester attempts to claim per recovery check. Higher values allow faster recovery but may cause uneven distribution."
    )]
    pub url_recovery_jobs_per_check: usize,
}

pub fn init() -> Config {
    if let Err(e) = load_config() {
        log::error!("Failed to load config {e}");
        // do nothing
    }
    let mut cfg = Config::init().expect("config init error");

    // set local mode
    if cfg.common.local_mode {
        cfg.common.node_role = "all".to_string();
        cfg.common.node_role_group = "".to_string();
    }
    cfg.common.is_local_storage = cfg.common.local_mode
        && (cfg.common.local_mode_storage == "disk" || cfg.common.local_mode_storage == "local");

    // check limit config
    if let Err(e) = check_limit_config(&mut cfg) {
        panic!("limit config error: {e}");
    }

    // check route config
    if let Err(e) = check_route_config(&cfg) {
        panic!("route config error: {e}");
    }

    // check common config
    if let Err(e) = check_common_config(&mut cfg) {
        panic!("common config error: {e}");
    }

    // check grpc config
    if let Err(e) = check_grpc_config(&mut cfg) {
        panic!("common config error: {e}");
    }

    // check http config
    if let Err(e) = check_http_config(&mut cfg) {
        panic!("common config error: {e}")
    }

    // check data path config
    if let Err(e) = check_path_config(&mut cfg) {
        panic!("data path config error: {e}");
    }

    // check memory cache
    if let Err(e) = check_memory_config(&mut cfg) {
        panic!("memory cache config error: {e}");
    }

    // check disk cache
    if let Err(e) = check_disk_cache_config(&mut cfg) {
        panic!("disk cache config error: {e}");
    }

    // check compact config
    if let Err(e) = check_compact_config(&mut cfg) {
        panic!("compact config error: {e}");
    }

    // check s3 config
    if let Err(e) = check_s3_config(&mut cfg) {
        panic!("s3 config error: {e}");
    }

    // check sns config
    if let Err(e) = check_sns_config(&mut cfg) {
        panic!("sns config error: {e}");
    }

    // check health check config
    if let Err(e) = check_health_check_config(&mut cfg) {
        panic!("health check config error: {e}");
    }

    // check pipeline config
    if let Err(e) = check_pipeline_config(&mut cfg) {
        panic!("pipeline config error: {e}");
    }

    // check nats config
    if let Err(e) = check_nats_config(&mut cfg) {
        panic!("nats config error: {e}");
    }

    // check inverted index config
    if let Err(e) = check_inverted_index_config(&mut cfg) {
        panic!("inverted index config error: {e}");
    }

    cfg
}

fn check_limit_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    // set real cpu num
    cfg.limit.real_cpu_num = max(1, sysinfo::get_cpu_limit());
    // limit cpu num by memory, 1 core per 1GB, in case the user only set memory
    // limit on k8s and we detect the whole node's cpu cores
    let mem_total = sysinfo::get_memory_limit();
    let cpu_num = if mem_total == 0 {
        cfg.limit.real_cpu_num
    } else {
        cfg.limit
            .real_cpu_num
            .min(max(1, mem_total / (1024 * 1024 * 1024)))
    };
    // set at least 2 threads
    let cpu_num = max(2, cpu_num);
    cfg.limit.cpu_num = cpu_num;
    if cfg.limit.http_worker_num == 0 {
        cfg.limit.http_worker_num = cpu_num;
    }
    if cfg.limit.http_worker_max_blocking == 0 {
        cfg.limit.http_worker_max_blocking = 256;
    }
    if cfg.limit.grpc_runtime_worker_num == 0 {
        cfg.limit.grpc_runtime_worker_num = cpu_num;
    }
    if cfg.limit.grpc_runtime_blocking_worker_num == 0 {
        cfg.limit.grpc_runtime_blocking_worker_num = 512;
    }
    if cfg.limit.job_runtime_worker_num == 0 {
        cfg.limit.job_runtime_worker_num = cpu_num;
    }
    if cfg.limit.job_runtime_blocking_worker_num == 0 {
        cfg.limit.job_runtime_blocking_worker_num = 512;
    }
    // HACK for thread_num equal to CPU core * 4
    if cfg.limit.query_thread_num == 0 {
        if cfg.common.local_mode {
            cfg.limit.query_thread_num = cpu_num;
        } else {
            cfg.limit.query_thread_num = cpu_num * 4;
        }
    }
    // per-file vix index evaluation is a handful of small IO waits plus
    // microseconds of CPU: overlap well beyond the core count by default
    if cfg.limit.vix_search_concurrency == 0 {
        cfg.limit.vix_search_concurrency = (cpu_num * 4).min(64);
    }
    cfg.limit.vix_search_concurrency = max(1, cfg.limit.vix_search_concurrency);

    if cfg.limit.file_download_thread_num == 0 {
        cfg.limit.file_download_thread_num = std::cmp::max(1, cpu_num / 2);
    }

    if cfg.limit.file_download_priority_queue_thread_num == 0 {
        cfg.limit.file_download_priority_queue_thread_num = std::cmp::max(1, cpu_num / 2);
    }

    // Co-located CPU-heavy role count (ingester + querier + compactor) — the
    // roles whose large default pools stack on a combined node. Parsed from
    // the node-role STRING because cluster::LOCAL_NODE is not yet initialized
    // while this Config is still being built (LOCAL_NODE depends on it); same
    // parse the later checks use. 1 in local mode / for a dedicated node.
    let cpu_role_div = if cfg.common.local_mode {
        1
    } else {
        let roles: Vec<cluster::Role> = cfg
            .common
            .node_role
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        let all = roles.contains(&cluster::Role::All);
        let mut n = 0;
        if all || roles.contains(&cluster::Role::Ingester) {
            n += 1;
        }
        if all || roles.contains(&cluster::Role::Querier) {
            n += 1;
        }
        if all || roles.contains(&cluster::Role::Compactor) {
            n += 1;
        }
        max(1, n)
    };
    // The WAL→storage move build pool. On a DEDICATED ingester keep it at the
    // full core count; on a COMBINED node divide by the co-located heavy-role
    // count so the move build, query fan-out and compaction merge pools sum to
    // ~cores instead of stacking. Measured (C2): scaling this 8→2 on a combined
    // node cut query p99 ~29% with unchanged ingest throughput (the move job is
    // not the ingest bottleneck). The IO-bound ZO_VIX_SEARCH_CONCURRENCY is
    // deliberately NOT scaled (measured: scaling it added no benefit and risks
    // starving the per-query fan-out on slow object storage).
    if cfg.limit.file_move_thread_num == 0 {
        cfg.limit.file_move_thread_num = max(1, cpu_num / cpu_role_div);
    }
    // HACK for file_merge_thread_num equal to CPU core
    if cfg.limit.file_merge_thread_num == 0 {
        if cfg.common.local_mode {
            cfg.limit.file_merge_thread_num = std::cmp::max(1, cpu_num / 2);
        } else {
            cfg.limit.file_merge_thread_num = cpu_num;
        }
    }
    // HACK for mem_dump_thread_num equal to CPU core
    if cfg.limit.mem_dump_thread_num == 0 {
        cfg.limit.mem_dump_thread_num = cpu_num;
    }
    // HACK for vortex_thread_num equal to CPU core
    if cfg.limit.vortex_thread_num == 0 {
        cfg.limit.vortex_thread_num = cpu_num;
    }
    // HACK for usage_reporting_thread_num equal to half of CPU core
    if cfg.limit.usage_reporting_thread_num == 0 {
        if cfg.common.local_mode {
            cfg.limit.usage_reporting_thread_num = std::cmp::max(1, cpu_num / 2);
        } else {
            cfg.limit.usage_reporting_thread_num = cpu_num;
        }
    }
    if cfg.limit.file_push_interval == 0 {
        cfg.limit.file_push_interval = 10;
    }
    if cfg.limit.file_push_limit == 0 {
        cfg.limit.file_push_limit = 10000;
    }

    if cfg.limit.sql_db_connections_min == 0 {
        cfg.limit.sql_db_connections_min = MINIMUM_DB_CONNECTIONS;
    }

    if cfg.limit.sql_db_connections_max == 0 {
        // auto = cpu*4 CAPPED at 32: uncapped, every 16-core pod could open
        // 64 conns per pool (× two pools) and a 50-pod fleet parked ~750
        // mostly-idle connections on the shared meta RDS (prod 2026-08-13).
        // An explicit env value still wins uncapped.
        cfg.limit.sql_db_connections_max = (cpu_num as u32 * 4).min(32);
    }
    cfg.limit.sql_db_connections_max =
        max(REQUIRED_DB_CONNECTIONS, cfg.limit.sql_db_connections_max);

    if cfg.limit.consistent_hash_vnodes < 1 {
        cfg.limit.consistent_hash_vnodes = 1000;
    }

    // reset to default if given zero
    if cfg.limit.max_dashboard_series < 1 {
        cfg.limit.max_dashboard_series = 100;
    }

    // check query timeout
    if cfg.limit.query_timeout == 0 {
        cfg.limit.query_timeout = 600;
    }
    if cfg.limit.query_ingester_timeout == 0 {
        cfg.limit.query_ingester_timeout = cfg.limit.query_timeout;
    }
    if cfg.limit.query_querier_timeout == 0 {
        cfg.limit.query_querier_timeout = cfg.limit.query_timeout;
    }

    // migrate deprecated *_file_retention ENVs to *_query_retention for backward compatibility
    // if the user explicitly set a non-hourly file retention, apply it to query retention
    if cfg.limit.logs_file_retention != "hourly" && cfg.limit.logs_query_retention == "hourly" {
        cfg.limit.logs_query_retention = cfg.limit.logs_file_retention.clone();
    }
    if cfg.limit.traces_file_retention != "hourly" && cfg.limit.traces_query_retention == "hourly" {
        cfg.limit.traces_query_retention = cfg.limit.traces_file_retention.clone();
    }
    if cfg.limit.metrics_file_retention != "hourly" && cfg.limit.metrics_query_retention == "hourly"
    {
        cfg.limit.metrics_query_retention = cfg.limit.metrics_file_retention.clone();
    }
    // file retention is always hourly now
    cfg.limit.logs_file_retention = "hourly".to_string();
    cfg.limit.traces_file_retention = "hourly".to_string();
    cfg.limit.metrics_file_retention = "hourly".to_string();

    // format ingest allowed upto and in future to micro
    cfg.limit.ingest_allowed_upto_micro = cfg.limit.ingest_allowed_upto * 3600 * 1_000_000;
    cfg.limit.ingest_allowed_in_future_micro =
        cfg.limit.ingest_allowed_in_future * 3600 * 1_000_000;

    // clamp batch_size to [1024, 8192]
    if cfg.limit.batch_size == 0 {
        cfg.limit.batch_size = 8192;
    }
    cfg.limit.batch_size = cfg.limit.batch_size.clamp(1024, 8192);
    // clamp datafusion_min_partition_num to 1
    cfg.limit.datafusion_min_partition_num = cfg.limit.datafusion_min_partition_num.max(1);

    // retain for atleast 1 hour
    if cfg.limit.workflow_error_retention_secs <= 3600 {
        cfg.limit.workflow_error_retention_secs = 3600;
    }

    Ok(())
}

fn check_route_config(cfg: &Config) -> Result<(), anyhow::Error> {
    if cfg.route.dispatch_strategy == RouteDispatchStrategy::Other {
        return Err(anyhow::anyhow!(
            "You must set ZO_ROUTE_STRATEGY to one of: workload (default) or random."
        ));
    }
    Ok(())
}

fn check_common_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    if cfg.limit.file_push_interval == 0 {
        cfg.limit.file_push_interval = 60;
    }
    if cfg.limit.req_cols_per_record_limit == 0 {
        cfg.limit.req_cols_per_record_limit = 65536;
    }
    // ingest admission projection factors must be at least 1x
    if cfg.common.ingest_admission_expansion_factor == 0 {
        cfg.common.ingest_admission_expansion_factor = 1;
    }
    if cfg.common.ingest_admission_compressed_factor == 0 {
        cfg.common.ingest_admission_compressed_factor = 1;
    }

    // check max_file_size_on_disk to MB
    if cfg.limit.max_file_size_on_disk == 0 {
        cfg.limit.max_file_size_on_disk = 512 * 1024 * 1024; // 512MB
    } else {
        cfg.limit.max_file_size_on_disk *= 1024 * 1024;
    }
    // check max_file_size_in_memory to MB
    if cfg.limit.max_file_size_in_memory == 0 {
        cfg.limit.max_file_size_in_memory = 512 * 1024 * 1024; // 512MB
    } else {
        cfg.limit.max_file_size_in_memory *= 1024 * 1024;
    }

    // check for metrics limit
    if cfg.limit.metrics_max_points_per_series == 0 {
        cfg.limit.metrics_max_points_per_series = 30_000;
    }
    if cfg.limit.metrics_cache_max_entries == 0 {
        cfg.limit.metrics_cache_max_entries = 10_000;
    }

    // check search job retention
    if cfg.limit.search_job_retention == 0 {
        return Err(anyhow::anyhow!("search job retention is set to zero"));
    }

    // segment-WAL knobs (DESIGN-SEGMENT-WAL.md): the flusher, builder, and
    // sweeper run whenever their node roles do, so these bounds must hold
    // regardless of ZO_INGEST_SEGMENT_MODE
    if cfg.common.segment_flush_interval_ms < 50 {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_FLUSH_INTERVAL_MS must be at least 50, got {}",
            cfg.common.segment_flush_interval_ms
        ));
    }
    if cfg.common.segment_flush_size_mb < 1 {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_FLUSH_SIZE_MB must be at least 1, got {}",
            cfg.common.segment_flush_size_mb
        ));
    }
    if cfg.common.segment_buffer_max_mb < 2 * cfg.common.segment_flush_size_mb {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_BUFFER_MAX_MB must be at least 2x ZO_SEGMENT_FLUSH_SIZE_MB ({}), got {}",
            2 * cfg.common.segment_flush_size_mb,
            cfg.common.segment_buffer_max_mb
        ));
    }
    if cfg.common.segment_build_batch < 1 {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_BUILD_BATCH must be at least 1, got {}",
            cfg.common.segment_build_batch
        ));
    }
    // A zero-sized chunk would make every non-empty contribution look
    // oversized. Keep the unit useful and let the byte conversion saturate.
    cfg.common.segment_build_chunk_mb = cfg.common.segment_build_chunk_mb.max(1);
    // M12 item 5: floor 1 — a zero would stall the small-build stream
    cfg.common.segment_build_concurrency = cfg.common.segment_build_concurrency.max(1);
    // Upload fan-out is bounded independently by count and payload bytes.
    cfg.common.segment_build_upload_concurrency =
        cfg.common.segment_build_upload_concurrency.max(1);
    cfg.common.segment_build_upload_max_inflight_mb =
        cfg.common.segment_build_upload_max_inflight_mb.max(1);
    // M13 item 1c: floor 1 — a zero would stall fetch+decode entirely
    cfg.common.segment_fetch_decode_concurrency =
        cfg.common.segment_fetch_decode_concurrency.max(1);
    if cfg.common.segment_build_lease_secs < 30 {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_BUILD_LEASE_SECS must be at least 30, got {}",
            cfg.common.segment_build_lease_secs
        ));
    }
    // beyond ~5 minutes the gate stops buying larger files and only grows the
    // per-query segment tail (and the sweeper's unbuilt-backlog warning fires
    // at 10 minutes — a config must not look like an outage)
    if cfg.common.segment_build_max_wait_secs > 300 {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_BUILD_MAX_WAIT_SECS must be at most 300, got {}",
            cfg.common.segment_build_max_wait_secs
        ));
    }
    // M13 aging lane: the ratio is a fraction of claim passes — clamp
    // silently (NaN and negatives disable the lane rather than erroring;
    // >1.0 means every engaged pass)
    if !cfg.common.segment_build_age_lane_ratio.is_finite()
        || cfg.common.segment_build_age_lane_ratio < 0.0
    {
        cfg.common.segment_build_age_lane_ratio = 0.0;
    } else if cfg.common.segment_build_age_lane_ratio > 1.0 {
        cfg.common.segment_build_age_lane_ratio = 1.0;
    }
    if cfg.common.segment_retain_secs < 60 {
        return Err(anyhow::anyhow!(
            "ZO_SEGMENT_RETAIN_SECS must be at least 60, got {}",
            cfg.common.segment_retain_secs
        ));
    }

    if (cfg.common.tracing_enabled || cfg.common.tracing_search_enabled)
        && cfg.common.otel_otlp_url.is_empty()
        && cfg.common.otel_otlp_grpc_url.is_empty()
    {
        return Err(anyhow::anyhow!(
            "Either grpc or http url should be set when enabling tracing"
        ));
    }

    // If tracing_extra_envs is empty, reset to default value
    if cfg.common.tracing_extra_envs.is_empty() {
        cfg.common.tracing_extra_envs =
            "K8S_CLUSTER,K8S_NAMESPACE_NAME,K8S_NODE_NAME,K8S_CONTAINER_NAME,K8S_POD_NAME"
                .to_string();
    }

    // HACK instance_name
    if cfg.common.instance_name.is_empty() {
        cfg.common.instance_name = sysinfo::os::get_hostname();
    }
    cfg.common.instance_name_short = cfg
        .common
        .instance_name
        .split('.')
        .next()
        .unwrap()
        .to_string();

    // HACK for tracing, always disable tracing except ingester and querier
    let local_node_role: Vec<cluster::Role> = cfg
        .common
        .node_role
        .clone()
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    if !local_node_role.contains(&cluster::Role::All)
        && !local_node_role.contains(&cluster::Role::Ingester)
        && !local_node_role.contains(&cluster::Role::Querier)
    {
        cfg.common.tracing_enabled = false;
    }

    if local_node_role.contains(&cluster::Role::ActionServer) {
        // action server does not have external dep, so can ignore their config check
        return Ok(());
    }

    // format local_mode_storage
    cfg.common.local_mode_storage = cfg.common.local_mode_storage.to_lowercase();

    check_file_format_config(cfg);

    // check queue store
    if cfg.common.queue_store.is_empty() {
        cfg.common.queue_store = "nats".to_string();
    }
    cfg.common.queue_store = cfg.common.queue_store.to_lowercase();
    if !cfg.common.queue_store.starts_with("nats") {
        return Err(anyhow::anyhow!("Queue store only supports nats."));
    }

    // format metadata storage
    if cfg.common.meta_store.is_empty() {
        if cfg.common.local_mode {
            cfg.common.meta_store = "sqlite".to_string();
        } else {
            cfg.common.meta_store = "nats".to_string();
        }
    }
    cfg.common.meta_store = cfg.common.meta_store.to_lowercase();
    if !cfg.common.local_mode && !cfg.common.meta_store.starts_with("postgres") {
        return Err(anyhow::anyhow!(
            "Meta store only supports postgres in cluster mode."
        ));
    }
    if cfg.common.meta_store.starts_with("postgres") && cfg.common.meta_postgres_dsn.is_empty() {
        let c = &cfg.common;
        if c.meta_postgres_host.is_empty()
            || c.meta_postgres_user.is_empty()
            || c.meta_postgres_password.is_empty()
            || c.meta_postgres_dbname.is_empty()
        {
            return Err(anyhow::anyhow!(
                "Meta store is PostgreSQL, you must set either ZO_META_POSTGRES_DSN or all of \
                 ZO_META_POSTGRES_HOST, ZO_META_POSTGRES_USER, ZO_META_POSTGRES_PASSWORD, \
                 ZO_META_POSTGRES_DBNAME"
            ));
        }
        // Compose the DSN from the individual vars. User, password and dbname are
        // percent-encoded so credentials with special characters survive the round
        // trip — sqlx percent-decodes them again when it parses the DSN.
        let dsn = format!(
            "postgres://{}:{}@{}:{}/{}",
            urlencoding::encode(&c.meta_postgres_user),
            urlencoding::encode(&c.meta_postgres_password),
            c.meta_postgres_host,
            c.meta_postgres_port,
            urlencoding::encode(&c.meta_postgres_dbname),
        );
        cfg.common.meta_postgres_dsn = dsn;
    }

    if cfg.common.meta_store.starts_with("mysql") {
        return Err(anyhow::anyhow!("We don't support MySQL anymore."));
    }

    // check meta partition mode
    if cfg.common.meta_partition_mode != "manual" {
        cfg.common.meta_partition_mode = "auto".to_string();
    }

    // If the default scrape interval is less than 5s, raise an error
    if cfg.common.default_scrape_interval < 5 {
        return Err(anyhow::anyhow!(
            "Default scrape interval can not be set to lesser than 5s ."
        ));
    }

    // migrate deprecated ZO_BLOOM_FILTER_DEFAULT_FIELDS into
    // ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS for backward compatibility
    #[allow(deprecated)]
    if !cfg.common.bloom_filter_default_fields.is_empty() {
        log::warn!(
            "ZO_BLOOM_FILTER_DEFAULT_FIELDS is deprecated and will be removed in v1.0.0, please use ZO_FEATURE_BLOOM_FILTER_EXTRA_FIELDS instead"
        );
        if cfg.common.feature_bloom_filter_extra_fields.is_empty() {
            cfg.common.feature_bloom_filter_extra_fields =
                cfg.common.bloom_filter_default_fields.clone();
        } else {
            cfg.common.feature_bloom_filter_extra_fields = format!(
                "{},{}",
                cfg.common.feature_bloom_filter_extra_fields,
                cfg.common.bloom_filter_default_fields
            );
        }
    }

    // check for join match one
    if cfg.common.feature_join_match_one_enabled && cfg.common.feature_join_right_side_max_rows == 0
    {
        cfg.common.feature_join_right_side_max_rows = 50_000;
    }

    // check for broadcast join left side max rows
    if cfg.common.feature_broadcast_join_enabled
        && cfg.common.feature_broadcast_join_left_side_max_rows == 0
    {
        cfg.common.feature_broadcast_join_left_side_max_rows = 10_000;
    }

    if cfg.common.feature_broadcast_join_enabled
        && cfg.common.feature_broadcast_join_left_side_max_size == 0
    {
        cfg.common.feature_broadcast_join_left_side_max_size = 10; // 10 MB
    }

    if cfg.common.default_hec_stream.is_empty() {
        cfg.common.default_hec_stream = "_hec".to_string();
    }

    if cfg.common.usage_publish_interval < 1 {
        cfg.common.usage_publish_interval = 60;
    }

    cfg.common.log_page_default_field_list = cfg.common.log_page_default_field_list.to_lowercase();
    if !matches!(
        cfg.common.log_page_default_field_list.as_str(),
        "all" | "interesting"
    ) {
        // legacy value "uds" (and anything unknown) now maps to all fields
        cfg.common.log_page_default_field_list = "all".to_string();
    }

    Ok(())
}

// Vortex data files are supported in all builds; nothing to normalize.
// `ZO_FILE_FORMAT` only selects the flat columnar data format
// (parquet/vortex); logs/traces are always core `.vix` files, so `vix`
// is normalized away here.
fn check_file_format_config(cfg: &mut Config) {
    if cfg.common.file_format == FileFormat::Vix {
        log::warn!(
            "ZO_FILE_FORMAT=vix is not a valid data-file format (logs/traces are always core \
             .vix files); falling back to parquet"
        );
        cfg.common.file_format = FileFormat::Parquet;
    }
}

fn check_grpc_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    if cfg.grpc.tls_enabled
        && (cfg.grpc.tls_cert_domain.is_empty()
            || cfg.grpc.tls_cert_path.is_empty()
            || cfg.grpc.tls_key_path.is_empty())
    {
        return Err(anyhow::anyhow!(
            "ZO_GRPC_TLS_CERT_DOMAIN, ZO_GRPC_TLS_CERT_PATH and ZO_GRPC_TLS_KEY_PATH must be set when ZO_GRPC_TLS_ENABLED is true"
        ));
    }
    Ok(())
}

fn check_http_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    if cfg.http.tls_enabled
        && (cfg.http.tls_cert_path.is_empty() || cfg.http.tls_key_path.is_empty())
    {
        return Err(anyhow::anyhow!(
            "When ZO_HTTP_TLS_ENABLED=true, both ZO_HTTP_TLS_CERT_PATH \
             and ZO_HTTP_TLS_KEY_PATH must be set."
        ));
    }
    Ok(())
}

fn check_path_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    // for web
    if cfg.common.web_url.ends_with('/') {
        cfg.common.web_url = cfg.common.web_url.trim_end_matches('/').to_string();
    }
    if cfg.common.base_uri.ends_with('/') {
        cfg.common.base_uri = cfg.common.base_uri.trim_end_matches('/').to_string();
    }
    // for data
    if cfg.common.data_dir.is_empty() {
        cfg.common.data_dir = "./data/openobserve/".to_string();
    }
    if !cfg.common.data_dir.ends_with('/') {
        cfg.common.data_dir = format!("{}/", cfg.common.data_dir);
    }
    if cfg.common.data_wal_dir.is_empty() {
        cfg.common.data_wal_dir = format!("{}wal/", cfg.common.data_dir);
    }
    if !cfg.common.data_wal_dir.ends_with('/') {
        cfg.common.data_wal_dir = format!("{}/", cfg.common.data_wal_dir);
    }
    if cfg.common.data_stream_dir.is_empty() {
        cfg.common.data_stream_dir = format!("{}stream/", cfg.common.data_dir);
    }
    if !cfg.common.data_stream_dir.ends_with('/') {
        cfg.common.data_stream_dir = format!("{}/", cfg.common.data_stream_dir);
    }
    if cfg.common.data_db_dir.is_empty() {
        cfg.common.data_db_dir = format!("{}db/", cfg.common.data_dir);
    }
    if !cfg.common.data_db_dir.ends_with('/') {
        cfg.common.data_db_dir = format!("{}/", cfg.common.data_db_dir);
    }
    if cfg.common.data_cache_dir.is_empty() {
        cfg.common.data_cache_dir = format!("{}cache/", cfg.common.data_dir);
    }
    if !cfg.common.data_cache_dir.ends_with('/') {
        cfg.common.data_cache_dir = format!("{}/", cfg.common.data_cache_dir);
    }
    if cfg.common.data_tmp_dir.is_empty() {
        cfg.common.data_tmp_dir = format!("{}tmp/", cfg.common.data_dir);
    }
    if !cfg.common.data_tmp_dir.ends_with('/') {
        cfg.common.data_tmp_dir = format!("{}/", cfg.common.data_tmp_dir);
    }
    if cfg.common.mmdb_data_dir.is_empty() {
        cfg.common.mmdb_data_dir = format!("{}mmdb/", cfg.common.data_dir);
    }
    if !cfg.common.mmdb_data_dir.ends_with('/') {
        cfg.common.mmdb_data_dir = format!("{}/", cfg.common.mmdb_data_dir);
    }

    Ok(())
}

fn check_memory_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    let mem_total = sysinfo::get_memory_limit();
    cfg.limit.mem_total = mem_total;
    if cfg.memory_cache.max_size == 0 {
        if cfg.common.local_mode {
            cfg.memory_cache.max_size = mem_total / 4; // 25%
        } else {
            cfg.memory_cache.max_size = mem_total / 2; // 50%
        }
    } else {
        cfg.memory_cache.max_size *= 1024 * 1024;
    }
    if cfg.memory_cache.skip_size == 0 {
        // will skip the cache when a query need cache great than this value, default is
        // 50% of max_size
        cfg.memory_cache.skip_size = cfg.memory_cache.max_size / 2;
    } else {
        cfg.memory_cache.skip_size *= 1024 * 1024;
    }
    if cfg.memory_cache.release_size == 0 {
        // when cache is full will release how many data once time, default is 10% of
        // max_size
        cfg.memory_cache.release_size = cfg.memory_cache.max_size / 10;
    } else {
        cfg.memory_cache.release_size *= 1024 * 1024;
    }
    if cfg.memory_cache.gc_size == 0 {
        cfg.memory_cache.gc_size = 100 * 1024 * 1024; // 100 MB
    } else {
        cfg.memory_cache.gc_size *= 1024 * 1024;
    }
    if cfg.memory_cache.enabled && cfg.memory_cache.max_size >= mem_total {
        return Err(anyhow::anyhow!(
            "ZO_MEMORY_CACHE_MAX_SIZE is larger than total memory, please set a smaller value"
        ));
    }
    let local_node_role: Vec<cluster::Role> = cfg
        .common
        .node_role
        .clone()
        .split(',')
        .map(|s| s.parse().unwrap())
        .collect();
    if cfg.memory_cache.datafusion_max_size == 0 {
        if local_node_role == [cluster::Role::Compactor] {
            // Merge contexts share one pool. Above DataFusion's 256 MiB
            // runtime floor, retain at least two thirds of the cgroup for
            // segment builds, VIX rebuilds, and native allocations; the old
            // two-worker divisor gave this shared pool half the pod.
            cfg.memory_cache.datafusion_max_size =
                mem_total / cfg.limit.file_merge_thread_num.max(3);
        } else if cfg.common.local_mode {
            cfg.memory_cache.datafusion_max_size = (mem_total - cfg.memory_cache.max_size) / 2; // 25%
        } else {
            cfg.memory_cache.datafusion_max_size = mem_total - cfg.memory_cache.max_size; // 50%
        }
    } else {
        cfg.memory_cache.datafusion_max_size *= 1024 * 1024;
    }

    if cfg.memory_cache.bucket_num == 0 {
        cfg.memory_cache.bucket_num = cfg.limit.cpu_num;
    }
    cfg.memory_cache.max_size /= cfg.memory_cache.bucket_num;
    cfg.memory_cache.release_size /= cfg.memory_cache.bucket_num;
    cfg.memory_cache.gc_size /= cfg.memory_cache.bucket_num;

    // for memtable limit check
    if cfg.limit.mem_table_max_size == 0 {
        if cfg.common.local_mode {
            cfg.limit.mem_table_max_size = mem_total / 4; // 25%
        } else {
            cfg.limit.mem_table_max_size = mem_total / 2; // 50%
        }
    } else {
        cfg.limit.mem_table_max_size *= 1024 * 1024;
    }
    if cfg.limit.mem_table_bucket_num == 0 {
        cfg.limit.mem_table_bucket_num = 1;
    }

    // wal
    if cfg.limit.wal_write_buffer_size < 4096 {
        cfg.limit.wal_write_buffer_size = 4096;
    }
    if cfg.limit.wal_write_queue_size == 0 {
        cfg.limit.wal_write_queue_size = 10000;
    }

    // check query settings
    if cfg.limit.query_group_base_speed == 0 {
        cfg.limit.query_group_base_speed = SIZE_IN_GB as usize;
    } else {
        cfg.limit.query_group_base_speed *= 1024 * 1024;
    }
    if cfg.limit.query_partition_by_secs == 0 {
        cfg.limit.query_partition_by_secs = 5;
    }
    if cfg.limit.query_partition_max_num == 0 {
        cfg.limit.query_partition_max_num = 100;
    }
    if cfg.limit.query_default_limit == 0 {
        cfg.limit.query_default_limit = 1000;
    }

    // The vix reader cache defaults to 10% of RAM with NO upper clamp (parsed
    // dictionaries are the working set of every hot query — an artificial cap
    // silently degrades hosts serving many files). Resolve it BEFORE the
    // legacy footer knob is defaulted so "legacy knob set, new knob unset"
    // still honors the operator's explicit value.
    if cfg.limit.vix_reader_cache_max_size == 0 {
        if cfg.limit.inverted_index_footer_cache_max_size > 0 {
            // compat: fall back to the explicitly-set legacy footer knob (MB)
            cfg.limit.vix_reader_cache_max_size =
                cfg.limit.inverted_index_footer_cache_max_size * (SIZE_IN_MB as usize);
        } else {
            cfg.limit.vix_reader_cache_max_size = (cfg.limit.mem_total as f64 * 0.10) as usize;
        }
    } else {
        cfg.limit.vix_reader_cache_max_size *= SIZE_IN_MB as usize;
    }
    if cfg.limit.inverted_index_footer_cache_max_size == 0 {
        cfg.limit.inverted_index_footer_cache_max_size =
            ((cfg.limit.mem_total as f64 / SIZE_IN_MB * 0.05) as usize).clamp(100, 1024)
                * (SIZE_IN_MB as usize);
    } else {
        cfg.limit.inverted_index_footer_cache_max_size *= SIZE_IN_MB as usize;
    }
    if cfg.limit.bloom_footer_cache_max_size == 0 {
        // 1% of total mem, clamped to [32, 256] MB. Bloom footers are an
        // order of magnitude smaller than inverted-index footers (footer
        // payload ≈ 24 B per file × 3 fields + per-field header ≈ 7.5 KB
        // per `.bf`), so the cache holds 4-32 K entries at this size.
        cfg.limit.bloom_footer_cache_max_size =
            ((cfg.limit.mem_total as f64 / SIZE_IN_MB * 0.01) as usize).clamp(32, 256)
                * (SIZE_IN_MB as usize);
    } else {
        cfg.limit.bloom_footer_cache_max_size *= SIZE_IN_MB as usize;
    }

    if cfg.limit.datafusion_file_stat_cache_max_size == 0 {
        cfg.limit.datafusion_file_stat_cache_max_size =
            ((cfg.limit.mem_total as f64 / SIZE_IN_MB * 0.05) as usize).clamp(100, 1024)
                * (SIZE_IN_MB as usize);
    } else {
        cfg.limit.datafusion_file_stat_cache_max_size *= SIZE_IN_MB as usize;
    }
    Ok(())
}

/// Strip the Windows extended-length prefix (`\\?\`) from a canonicalized path
/// so it can be compared with sysinfo mount points that use the plain DOS form.
///
/// Uses [`std::path::Prefix`] to detect verbatim prefixes rather than
/// manipulating the string directly, which would silently break on non-ASCII
/// drive letters or UNC paths.
pub fn deverbatim(path: &Path) -> std::borrow::Cow<'_, str> {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};
        if let Some(Component::Prefix(p)) = path.components().next() {
            if let Prefix::VerbatimDisk(drive) = p.kind() {
                // \\?\C:\rest → C:\rest
                // p.as_os_str() is "\\?\C:" (6 bytes); the remainder of the
                // original string is "\rest", so prepend the plain drive letter.
                let after_prefix = &path.to_string_lossy()[p.as_os_str().len()..];
                return format!("{}:{}", drive as char, after_prefix).into();
            }
        }
    }
    path.to_string_lossy()
}

fn check_disk_cache_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(&cfg.common.data_cache_dir).expect("create cache dir success");
    let cache_dir_path = Path::new(&cfg.common.data_cache_dir)
        .canonicalize()
        .unwrap();
    let cache_dir_owned = deverbatim(&cache_dir_path).into_owned();
    let cache_dir = cache_dir_owned.as_str();

    // disable disk cache for local disk storage
    if cfg.common.is_local_storage
        && !cfg.common.result_cache_enabled
        && !cfg.common.feature_query_streaming_aggs
    {
        cfg.disk_cache.enabled = false;
    }

    // disable result cache if disk cache is disabled
    if !cfg.disk_cache.enabled {
        cfg.common.result_cache_enabled = false;
        cfg.common.feature_query_streaming_aggs = false;
    }

    let disks = sysinfo::disk::get_disk_usage();
    let disk = disks.iter().find(|d| cache_dir.starts_with(&d.mount_point));
    let (disk_total, disk_free) = match disk {
        Some(d) => (d.total_space, d.available_space),
        None => (0, 0),
    };
    cfg.limit.disk_total = disk_total as usize;
    cfg.limit.disk_free = disk_free as usize;
    if cfg.disk_cache.max_size == 0 {
        // Add the current cache directory size back to free space so the limit is
        // stable across restarts.  Without this correction the measured "free" space
        // shrinks every time the app restarts with a full cache, causing the limit to
        // drift lower on each startup.
        let cache_current_size = crate::utils::file::get_dir_size(cache_dir);
        let effective_free = cfg.limit.disk_free + cache_current_size;
        cfg.disk_cache.max_size = effective_free / 2; // 50%
        if cfg.disk_cache.max_size > 1024 * 1024 * 1024 * 500 {
            cfg.disk_cache.max_size = 1024 * 1024 * 1024 * 500; // 500GB
        }
    } else {
        cfg.disk_cache.max_size *= 1024 * 1024;
    }

    if cfg.disk_cache.result_max_size == 0 {
        cfg.disk_cache.result_max_size = cfg.disk_cache.max_size / 10; // 10%
        if cfg.disk_cache.result_max_size > 1024 * 1024 * 1024 * 20 {
            cfg.disk_cache.result_max_size = 1024 * 1024 * 1024 * 20; // 20GB
        }
    } else {
        cfg.disk_cache.result_max_size *= 1024 * 1024;
    }

    if cfg.disk_cache.aggregation_max_size == 0 {
        cfg.disk_cache.aggregation_max_size = cfg.disk_cache.max_size / 10; // 10%
        if cfg.disk_cache.aggregation_max_size > 1024 * 1024 * 1024 * 20 {
            cfg.disk_cache.aggregation_max_size = 1024 * 1024 * 1024 * 20; // 20GB
        }
    } else {
        cfg.disk_cache.aggregation_max_size *= 1024 * 1024;
    }

    if cfg.disk_cache.skip_size == 0 {
        // will skip the cache when a query need cache great than this value, default is
        // 50% of max_size
        cfg.disk_cache.skip_size = cfg.disk_cache.max_size / 2;
    } else {
        cfg.disk_cache.skip_size *= 1024 * 1024;
    }
    if cfg.disk_cache.release_size == 0 {
        // when cache is full will release how many data once time, default is 10% of
        // max_size
        cfg.disk_cache.release_size = cfg.disk_cache.max_size / 10;
    } else {
        cfg.disk_cache.release_size *= 1024 * 1024;
    }
    if cfg.disk_cache.gc_size == 0 {
        cfg.disk_cache.gc_size = 100 * 1024 * 1024; // 100 MB
    } else {
        cfg.disk_cache.gc_size *= 1024 * 1024;
    }

    if cfg.disk_cache.multi_dir.contains('/') {
        return Err(anyhow::anyhow!(
            "ZO_DISK_CACHE_MULTI_DIR only supports a single directory level, can not contains / "
        ));
    }

    if cfg.disk_cache.bucket_num == 0 {
        // because we validate cpu_num before this
        // we can be sure here that value is sane.

        // following numbers are imperically decided, users can set the value
        // directly if they know better, otherwise this was the best numbers
        // for bucket_num based on thread count.
        let threads = cfg.limit.cpu_num;
        if threads <= 16 {
            // for less than 16 threads, same buckets would be good enough
            // with 16 files in parallel we should not run into that many
            // files going into same bucket, so ok.
            cfg.disk_cache.bucket_num = threads;
        } else if threads > 16 && threads <= 64 {
            // for 32 -> 64 ish range, there can be a lot of collisions
            // so we set it to double the threads to avoid any collisions
            cfg.disk_cache.bucket_num = 2 * threads;
        } else {
            // for > 64 threads, it was observed that even with 1.5 times buckets
            // it is ok, not that many collisions. This is imperical, no concrete
            // reasoning for 1.5
            cfg.disk_cache.bucket_num = (threads as f64 * 1.5) as usize;
        }
    }
    cfg.disk_cache.bucket_num = max(
        cfg.disk_cache.bucket_num,
        cfg.disk_cache
            .multi_dir
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .count(),
    );
    cfg.disk_cache.max_size /= cfg.disk_cache.bucket_num;
    cfg.disk_cache.result_max_size /= cfg.disk_cache.bucket_num;
    cfg.disk_cache.aggregation_max_size /= cfg.disk_cache.bucket_num;
    cfg.disk_cache.release_size /= cfg.disk_cache.bucket_num;
    cfg.disk_cache.gc_size /= cfg.disk_cache.bucket_num;

    Ok(())
}

fn check_compact_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    // M19: any retention >= 1 day is valid (0/negative = disabled). The old
    // 3-day floor predates the v2 era: the fleet runs engine-owned 1-day
    // retention as the PRIMARY row+object lifecycle, with the S3 lifecycle
    // rules kept at 2 days purely as the safety net — a 1-day setting must
    // not be rejected at startup.
    if cfg.compact.interval < 1 {
        cfg.compact.interval = 10;
    }

    // Convert compaction size limits from configured MB to runtime bytes.
    // Indexed trace-core inputs have a separate higher ceiling because their
    // dictionary-passthrough merge does not pay the index-less rebuild's
    // input-proportional memory. Zero inherits the global ceiling.
    if cfg.compact.max_file_size < 1 {
        cfg.compact.max_file_size = 512;
    }
    cfg.compact.max_file_size *= 1024 * 1024;
    if cfg.compact.traces_indexed_max_file_size > 0 {
        cfg.compact.traces_indexed_max_file_size =
            (cfg.compact.traces_indexed_max_file_size * 1024 * 1024).max(cfg.compact.max_file_size);
    }
    if cfg.compact.delete_files_delay_hours < 1 {
        cfg.compact.delete_files_delay_hours = 2;
    }

    if cfg.compact.data_retention_interval < 1 {
        cfg.compact.data_retention_interval = 3600;
    }
    if cfg.compact.old_data_interval < 1 {
        cfg.compact.old_data_interval = 3600;
    }
    if cfg.compact.old_data_max_days < 1 {
        cfg.compact.old_data_max_days = 7;
    }
    if cfg.compact.old_data_min_hours < 1 {
        cfg.compact.old_data_min_hours = 2;
    }
    if cfg.compact.old_data_min_files < 1 {
        cfg.compact.old_data_min_files = 10;
    }
    if cfg.compact.file_list_deleted_batch_size == 0 {
        cfg.compact.file_list_deleted_batch_size = 1000;
    }
    if cfg.compact.batch_size < 1 {
        cfg.compact.batch_size = 100;
    }
    if cfg.compact.pending_jobs_metric_interval == 0 {
        cfg.compact.pending_jobs_metric_interval = 300;
    }
    if !cfg.compact.fast_mode && cfg.common.local_mode {
        cfg.compact.fast_mode = true;
    }

    Ok(())
}

fn check_sns_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    // Validate endpoint URL if provided
    if !cfg.sns.endpoint.is_empty()
        && !cfg.sns.endpoint.starts_with("http://")
        && !cfg.sns.endpoint.starts_with("https://")
    {
        return Err(anyhow::anyhow!(
            "Invalid SNS endpoint URL. It must start with http:// or https://"
        ));
    }

    // Validate timeouts
    if cfg.sns.connect_timeout == 0 {
        cfg.sns.connect_timeout = 10; // Default to 10 seconds if not set
        log::warn!("SNS connect timeout not specified, defaulting to 10 seconds");
    }
    if cfg.sns.operation_timeout == 0 {
        cfg.sns.operation_timeout = 30; // Default to 30 seconds if not set
        log::warn!("SNS operation timeout not specified, defaulting to 30 seconds");
    }

    Ok(())
}

fn check_s3_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    // Ensure each bucket prefix ends with '/' for multi-bucket configurations
    if !cfg.s3.bucket_prefix.is_empty() {
        let prefixes: Vec<String> = cfg
            .s3
            .bucket_prefix
            .split(',')
            .map(|prefix| {
                let trimmed = prefix.trim();
                if trimmed.is_empty() || trimmed.ends_with('/') {
                    trimmed.to_string()
                } else {
                    format!("{}/", trimmed)
                }
            })
            .collect();
        cfg.s3.bucket_prefix = prefixes.join(",");
    }
    if cfg.s3.provider.is_empty() {
        if cfg.s3.server_url.contains(".googleapis.com") {
            cfg.s3.provider = "gcs".to_string();
        } else if cfg.s3.server_url.contains(".aliyuncs.com") {
            cfg.s3.provider = "oss".to_string();
            if !cfg
                .s3
                .server_url
                .contains(&format!("://{}.", cfg.s3.bucket_name))
            {
                cfg.s3.server_url = cfg
                    .s3
                    .server_url
                    .replace("://", &format!("://{}.", cfg.s3.bucket_name));
            }
        } else {
            cfg.s3.provider = "aws".to_string();
        }
    }
    cfg.s3.provider = cfg.s3.provider.to_lowercase();
    if cfg.s3.provider.eq("swift") {
        unsafe { std::env::set_var("AWS_EC2_METADATA_DISABLED", "true") };
    }

    if cfg.s3.keepalive_timeout == 0 {
        // reset to default
        cfg.s3.keepalive_timeout = 20;
    }

    Ok(())
}

fn check_pipeline_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    // pipeline
    if cfg.pipeline.remote_stream_wal_dir.is_empty() {
        cfg.pipeline.remote_stream_wal_dir = format!("{}remote_stream_wal/", cfg.common.data_dir);
    }

    if !cfg.pipeline.remote_stream_wal_dir.is_empty()
        && !cfg.pipeline.remote_stream_wal_dir.ends_with('/')
    {
        cfg.pipeline.remote_stream_wal_dir = format!("{}/", cfg.pipeline.remote_stream_wal_dir);
    }

    if cfg.pipeline.offset_flush_interval == 0 {
        cfg.pipeline.offset_flush_interval = 10;
    }
    if cfg.pipeline.remote_request_max_retry_time == 0 {
        cfg.pipeline.remote_request_max_retry_time = 86400; // 24 hours, in seconds
    }

    if cfg.pipeline.wal_size_limit == 0 {
        cfg.pipeline.wal_size_limit = cfg.limit.disk_free as u64 / 2; // 50%
        if cfg.pipeline.wal_size_limit > 1024 * 1024 * 1024 * 100 {
            cfg.pipeline.wal_size_limit = 1024 * 1024 * 1024 * 100; // 100GB
        }
    } else {
        cfg.pipeline.wal_size_limit *= 1024 * 1024;
    }

    if cfg.pipeline.pipeline_file_push_back_interval == 0 {
        cfg.pipeline.pipeline_file_push_back_interval = 2; // 2 seconds
    }

    if cfg.pipeline.pipeline_sink_task_spawn_interval_ms == 0 {
        cfg.pipeline.pipeline_sink_task_spawn_interval_ms = 100; // 100 milliseconds
    }
    Ok(())
}

fn check_health_check_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    if cfg.health_check.timeout == 0 {
        cfg.health_check.timeout = 5;
    }
    if cfg.health_check.failed_times == 0 {
        cfg.health_check.failed_times = 3;
    }
    Ok(())
}

#[inline]
pub fn is_local_disk_storage() -> bool {
    get_config().common.is_local_storage
}

#[inline]
pub fn get_cluster_name() -> String {
    let cfg = get_config();
    if !cfg.common.cluster_name.is_empty() {
        cfg.common.cluster_name.to_string()
    } else {
        INSTANCE_ID.get("instance_id").unwrap().to_string()
    }
}

#[inline]
pub fn get_parquet_compression(compression: &str) -> parquet::basic::Compression {
    match compression.to_lowercase().as_str() {
        "none" | "uncompressed" => parquet::basic::Compression::UNCOMPRESSED,
        "snappy" => parquet::basic::Compression::SNAPPY,
        "gzip" => parquet::basic::Compression::GZIP(Default::default()),
        "brotli" => parquet::basic::Compression::BROTLI(Default::default()),
        "lz4" | "lz4_raw" => parquet::basic::Compression::LZ4_RAW,
        "zstd" => parquet::basic::Compression::ZSTD(Default::default()),
        _ => parquet::basic::Compression::ZSTD(Default::default()),
    }
}

fn check_nats_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    if cfg.nats.queue_max_size == 0 {
        cfg.nats.queue_max_size = 2048; // 2GB
    }
    cfg.nats.queue_max_size *= 1024 * 1024; // convert to bytes
    Ok(())
}

fn check_inverted_index_config(cfg: &mut Config) -> Result<(), anyhow::Error> {
    cfg.common.vix_read_mode = cfg.common.vix_read_mode.trim().to_lowercase();
    if cfg.common.vix_read_mode.is_empty() {
        cfg.common.vix_read_mode = "ranged".to_string();
    }
    if !matches!(cfg.common.vix_read_mode.as_str(), "cached" | "ranged") {
        return Err(anyhow::anyhow!(
            "ZO_VIX_READ_MODE must be 'cached' or 'ranged', got {:?}",
            cfg.common.vix_read_mode
        ));
    }
    cfg.common.vix_merge_type_policy = cfg.common.vix_merge_type_policy.trim().to_lowercase();
    if cfg.common.vix_merge_type_policy.is_empty() {
        cfg.common.vix_merge_type_policy = "legacy".to_string();
    }
    if !matches!(
        cfg.common.vix_merge_type_policy.as_str(),
        "legacy" | "latest_schema"
    ) {
        return Err(anyhow::anyhow!(
            "ZO_VIX_MERGE_TYPE_POLICY must be 'legacy' or 'latest_schema', got {:?}",
            cfg.common.vix_merge_type_policy
        ));
    }
    if cfg.limit.inverted_index_result_cache_max_entries == 0 {
        cfg.limit.inverted_index_result_cache_max_entries = 100000;
    }
    if cfg.limit.inverted_index_result_cache_max_entry_size == 0 {
        cfg.limit.inverted_index_result_cache_max_entry_size = 524288;
    }
    if cfg.limit.inverted_index_result_cache_max_size == 0 {
        cfg.limit.inverted_index_result_cache_max_size = 256; // MB
    }
    if cfg.limit.inverted_index_skip_threshold == 0 {
        cfg.limit.inverted_index_skip_threshold = 35;
    }
    if cfg.limit.inverted_index_min_token_length == 0 {
        cfg.limit.inverted_index_min_token_length = 2;
    }
    if cfg.limit.inverted_index_max_token_length == 0 {
        cfg.limit.inverted_index_max_token_length = 64;
    }
    Ok(())
}

pub fn ensure_not_empty(s: &str, name: &str) -> Result<(), anyhow::Error> {
    if s.trim().is_empty() {
        return Err(anyhow::anyhow!("{} is empty", name));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M11 launch defaults (OWNER 2026-08-18: "cache_latest_files default
    /// to true — we need cache latest files"): caching + merge-input
    /// eviction ON, peer-to-peer fill explicitly HELD BACK.
    #[test]
    fn cache_latest_files_defaults_m11() {
        let cfg = Config::init().unwrap();
        assert!(cfg.cache_latest_files.enabled, "M11: caching defaults ON");
        assert!(
            cfg.cache_latest_files.cache_parquet,
            "M11: data-file sub-flag ON (.vix data + .vxi sidecar)"
        );
        assert!(
            cfg.cache_latest_files.delete_merge_files,
            "M11: merge inputs evicted when replaced"
        );
        assert!(
            !cfg.cache_latest_files.download_from_node,
            "owner holds peer-to-peer fill back — must stay OFF"
        );
    }

    /// M12 item 5 / M17: the per-pod L0 build concurrency is env-tunable
    /// (`ZO_SEGMENT_BUILD_CONCURRENCY`) — default 16 since M17 (it became
    /// the SECONDARY count cap under the byte-budget admission), floor 1
    /// (a zero would stall the small-build stream). One test covers
    /// default + override + floor sequentially: env vars are
    /// process-global, so splitting these into parallel tests would race.
    #[test]
    fn segment_build_concurrency_default_and_override_m12() {
        let key = "ZO_SEGMENT_BUILD_CONCURRENCY";
        // default: 16 (M17 — the byte budget binds, the count cap is wide)
        unsafe { std::env::remove_var(key) };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_concurrency, 16);

        // override wins
        unsafe { std::env::set_var(key, "8") };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_concurrency, 8);

        // floor: 0 clamps to 1 (mirrors check_common_config)
        unsafe { std::env::set_var(key, "0") };
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(cfg.common.segment_build_concurrency, 1);

        unsafe { std::env::remove_var(key) };
    }

    /// L0 uploads are independently bounded by file count and admitted
    /// payload MiB. Defaults, overrides and zero floors are covered in one
    /// process-global environment test.
    #[test]
    fn segment_build_upload_limits_default_override_and_floor() {
        let concurrency_key = "ZO_SEGMENT_BUILD_UPLOAD_CONCURRENCY";
        let bytes_key = "ZO_SEGMENT_BUILD_UPLOAD_MAX_INFLIGHT_MB";
        unsafe {
            std::env::remove_var(concurrency_key);
            std::env::remove_var(bytes_key);
        }
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_upload_concurrency, 8);
        assert_eq!(cfg.common.segment_build_upload_max_inflight_mb, 256);

        unsafe {
            std::env::set_var(concurrency_key, "3");
            std::env::set_var(bytes_key, "96");
        }
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_upload_concurrency, 3);
        assert_eq!(cfg.common.segment_build_upload_max_inflight_mb, 96);

        unsafe {
            std::env::set_var(concurrency_key, "0");
            std::env::set_var(bytes_key, "0");
        }
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(cfg.common.segment_build_upload_concurrency, 1);
        assert_eq!(cfg.common.segment_build_upload_max_inflight_mb, 1);

        unsafe {
            std::env::remove_var(concurrency_key);
            std::env::remove_var(bytes_key);
        }
    }

    /// M17 item 3: the byte-budget knob — default 0 = auto (40% of
    /// detected memory, resolved at the consumer), override in MB wins.
    #[test]
    fn segment_build_memory_budget_env_m17() {
        let key = "ZO_SEGMENT_BUILD_MEMORY_BUDGET_MB";
        unsafe { std::env::remove_var(key) };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_memory_budget_mb, 0, "0 = auto");

        unsafe { std::env::set_var(key, "6144") };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_memory_budget_mb, 6144);

        unsafe { std::env::remove_var(key) };
    }

    /// Direct L0 chunk sizing is 128 MiB by default, accepts an environment
    /// override, floors zero, and converts without overflow.
    #[test]
    fn segment_build_chunk_default_override_and_safe_bytes() {
        let key = "ZO_SEGMENT_BUILD_CHUNK_MB";
        unsafe { std::env::remove_var(key) };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_chunk_mb, 128);
        assert_eq!(cfg.common.segment_build_chunk_bytes(), 128 * 1024 * 1024);

        unsafe { std::env::set_var(key, "512") };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_chunk_mb, 512);
        assert_eq!(cfg.common.segment_build_chunk_bytes(), 512 * 1024 * 1024);

        unsafe { std::env::set_var(key, "0") };
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(cfg.common.segment_build_chunk_mb, 1);
        assert_eq!(cfg.common.segment_build_chunk_bytes(), 1024 * 1024);

        cfg.common.segment_build_chunk_mb = usize::MAX;
        assert_eq!(cfg.common.segment_build_chunk_bytes(), usize::MAX);
        unsafe { std::env::remove_var(key) };
    }

    /// M13 (1c): fetch+decode concurrency env — default 2 (the pre-M13
    /// hardcoded constant, byte-for-byte behavior), override wins, floor 1
    /// clamped at load. One test because env is process-global.
    #[test]
    fn segment_fetch_decode_concurrency_default_override_floor_m13() {
        let key = "ZO_SEGMENT_FETCH_DECODE_CONCURRENCY";
        unsafe { std::env::remove_var(key) };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_fetch_decode_concurrency, 2);

        unsafe { std::env::set_var(key, "8") };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_fetch_decode_concurrency, 8);

        unsafe { std::env::set_var(key, "0") };
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.common.segment_fetch_decode_concurrency, 1,
            "zero would stall fetch+decode; the floor clamps to 1"
        );

        unsafe { std::env::remove_var(key) };
    }

    /// M13 aging lane envs: safe defaults (6h / every 4th pass), override,
    /// and the [0,1] ratio clamp — one test because env is process-global.
    #[test]
    fn segment_build_age_lane_defaults_and_clamp_m13() {
        let secs_key = "ZO_SEGMENT_BUILD_AGE_LANE_SECS";
        let ratio_key = "ZO_SEGMENT_BUILD_AGE_LANE_RATIO";
        unsafe { std::env::remove_var(secs_key) };
        unsafe { std::env::remove_var(ratio_key) };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_age_lane_secs, 21600);
        assert!((cfg.common.segment_build_age_lane_ratio - 0.25).abs() < f64::EPSILON);

        // overrides win
        unsafe { std::env::set_var(secs_key, "3600") };
        unsafe { std::env::set_var(ratio_key, "0.5") };
        let cfg = Config::init().unwrap();
        assert_eq!(cfg.common.segment_build_age_lane_secs, 3600);
        assert!((cfg.common.segment_build_age_lane_ratio - 0.5).abs() < f64::EPSILON);

        // ratio clamps into [0, 1] (mirrors check_common_config)
        unsafe { std::env::set_var(ratio_key, "3.0") };
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();
        assert!((cfg.common.segment_build_age_lane_ratio - 1.0).abs() < f64::EPSILON);
        unsafe { std::env::set_var(ratio_key, "-0.5") };
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(cfg.common.segment_build_age_lane_ratio, 0.0);

        unsafe { std::env::remove_var(secs_key) };
        unsafe { std::env::remove_var(ratio_key) };
    }

    #[test]
    fn test_config_static_uses_std_lazylock_api() {
        let cfg = std::sync::LazyLock::force(&CONFIG).load();
        assert_eq!(
            cfg.limit.req_cols_per_record_limit,
            get_config().limit.req_cols_per_record_limit
        );
    }

    #[test]
    fn test_get_config() {
        let mut cfg = Config::init().unwrap();
        let ret = check_limit_config(&mut cfg);
        assert!(ret.is_ok());

        cfg.s3.server_url = "https://storage.googleapis.com".to_string();
        cfg.s3.provider = "".to_string();
        check_s3_config(&mut cfg).unwrap();
        assert_eq!(cfg.s3.provider, "gcs");
        cfg.s3.server_url = "https://oss-cn-beijing.aliyuncs.com".to_string();
        cfg.s3.provider = "".to_string();
        check_s3_config(&mut cfg).unwrap();
        assert_eq!(cfg.s3.provider, "oss");
        cfg.s3.server_url = "".to_string();
        cfg.s3.provider = "".to_string();
        check_s3_config(&mut cfg).unwrap();
        assert_eq!(cfg.s3.provider, "aws");

        // SNS configuration tests
        // Test default values
        check_sns_config(&mut cfg).unwrap();
        assert_eq!(cfg.sns.connect_timeout, 10);
        assert_eq!(cfg.sns.operation_timeout, 30);
        assert!(cfg.sns.endpoint.is_empty());

        // Test custom endpoint
        cfg.sns.endpoint = "https://sns.us-west-2.amazonaws.com".to_string();
        check_sns_config(&mut cfg).unwrap();
        assert_eq!(cfg.sns.endpoint, "https://sns.us-west-2.amazonaws.com");

        // Test custom timeouts
        cfg.sns.connect_timeout = 15;
        cfg.sns.operation_timeout = 45;
        check_sns_config(&mut cfg).unwrap();
        assert_eq!(cfg.sns.connect_timeout, 15);
        assert_eq!(cfg.sns.operation_timeout, 45);

        // Test zero values (should set to defaults)
        cfg.sns.connect_timeout = 0;
        cfg.sns.operation_timeout = 0;
        check_sns_config(&mut cfg).unwrap();
        assert_eq!(cfg.sns.connect_timeout, 10);
        assert_eq!(cfg.sns.operation_timeout, 30);

        // Test endpoint URL validation
        cfg.sns.endpoint = "invalid-url".to_string();
        assert!(check_sns_config(&mut cfg).is_err());

        cfg.memory_cache.max_size = 1024;
        cfg.memory_cache.release_size = 1024;
        cfg.memory_cache.bucket_num = 1;
        check_memory_config(&mut cfg).unwrap();
        assert_eq!(cfg.memory_cache.max_size, 1024 * 1024 * 1024);
        assert_eq!(cfg.memory_cache.release_size, 1024 * 1024 * 1024);

        let mut compactor_cfg = Config::init().unwrap();
        compactor_cfg.common.node_role = cluster::Role::Compactor.to_string();
        compactor_cfg.memory_cache.datafusion_max_size = 0;
        compactor_cfg.memory_cache.bucket_num = 1;
        compactor_cfg.limit.file_merge_thread_num = 2;
        check_memory_config(&mut compactor_cfg).unwrap();
        assert_eq!(
            compactor_cfg.memory_cache.datafusion_max_size,
            compactor_cfg.limit.mem_total / 3
        );

        cfg.limit.file_push_interval = 0;
        cfg.limit.req_cols_per_record_limit = 0;
        cfg.compact.interval = 0;
        cfg.compact.data_retention_days = 10;
        let ret = check_common_config(&mut cfg);
        assert!(ret.is_ok());
        assert_eq!(cfg.compact.data_retention_days, 10);
        assert_eq!(cfg.limit.req_cols_per_record_limit, 65536);

        // M19: short engine retention is valid (1-day is the v2 ops plan)
        cfg.compact.data_retention_days = 2;
        let ret = check_compact_config(&mut cfg);
        assert!(ret.is_ok());
        assert_eq!(cfg.compact.data_retention_days, 2);

        cfg.common.data_dir = "".to_string();
        let ret = check_path_config(&mut cfg);
        assert!(ret.is_ok());

        cfg.common.data_dir = "/abc".to_string();
        cfg.common.data_wal_dir = "/abc".to_string();
        cfg.common.data_stream_dir = "/abc".to_string();
        cfg.common.base_uri = "/abc/".to_string();
        let ret = check_path_config(&mut cfg);
        assert!(ret.is_ok());
        assert_eq!(cfg.common.data_dir, "/abc/".to_string());
        assert_eq!(cfg.common.data_wal_dir, "/abc/".to_string());
        assert_eq!(cfg.common.data_stream_dir, "/abc/".to_string());
        assert_eq!(cfg.common.data_dir, "/abc/".to_string());
        assert_eq!(cfg.common.base_uri, "/abc".to_string());

        cfg.common.base_uri = "/".to_string();
        let ret = check_path_config(&mut cfg);
        assert!(ret.is_ok());
        assert_eq!(cfg.common.base_uri, "".to_string());

        // Test route dispatch strategies
        cfg.route.dispatch_strategy = RouteDispatchStrategy::Workload;
        assert!(check_route_config(&cfg).is_ok());

        cfg.route.dispatch_strategy = RouteDispatchStrategy::Random;
        assert!(check_route_config(&cfg).is_ok());

        cfg.route.dispatch_strategy = RouteDispatchStrategy::Other;
        assert!(check_route_config(&cfg).is_err());
    }

    #[test]
    fn test_usage_report_to_own_org_field_exists() {
        // Test that usage_report_to_own_org field exists and is accessible
        let cfg = Config::init().unwrap();
        // Verify the field is accessible as a boolean
        let _value: bool = cfg.common.usage_report_to_own_org;
        // Test passes if we can access the field without error
    }

    #[test]
    fn test_usage_report_to_own_org_env_override() {
        // Test that environment variable can override the default
        unsafe {
            std::env::set_var("ZO_USAGE_REPORT_TO_OWN_ORG", "false");
        }
        let cfg = Config::init().unwrap();
        // Note: This test may fail if the config is already loaded
        // In that case, we just verify the field exists
        let _ = cfg.common.usage_report_to_own_org;
        unsafe {
            std::env::remove_var("ZO_USAGE_REPORT_TO_OWN_ORG");
        }
    }

    #[test]
    fn test_ensure_not_empty_valid() {
        assert!(ensure_not_empty("valid", "TEST").is_ok());
    }

    #[test]
    fn test_ensure_not_empty_invalid() {
        assert!(ensure_not_empty("", "TEST").is_err());
    }

    #[test]
    fn test_ensure_not_empty_with_whitespace() {
        assert!(ensure_not_empty("  value  ", "TEST").is_ok());
    }

    #[test]
    fn test_ensure_not_empty_single_char() {
        assert!(ensure_not_empty("a", "TEST").is_ok());
    }

    #[test]
    fn test_file_format_display() {
        assert_eq!(FileFormat::Parquet.to_string(), "parquet");
        assert_eq!(FileFormat::Vortex.to_string(), "vortex");
        assert_eq!(FileFormat::Vix.to_string(), "vix");
    }

    #[test]
    fn test_file_format_from_str() {
        assert_eq!(
            "parquet".parse::<FileFormat>().unwrap(),
            FileFormat::Parquet
        );
        assert_eq!(
            "PARQUET".parse::<FileFormat>().unwrap(),
            FileFormat::Parquet
        );
        assert_eq!("vortex".parse::<FileFormat>().unwrap(), FileFormat::Vortex);
        assert_eq!("VORTEX".parse::<FileFormat>().unwrap(), FileFormat::Vortex);
        assert_eq!("vix".parse::<FileFormat>().unwrap(), FileFormat::Vix);
        assert!("unknown".parse::<FileFormat>().is_err());
    }

    #[test]
    fn test_file_format_extension() {
        assert_eq!(FileFormat::Parquet.extension(), ".parquet");
        assert_eq!(FileFormat::Vortex.extension(), ".vortex");
        assert_eq!(FileFormat::Vix.extension(), ".vix");
    }

    #[test]
    fn test_file_format_for_ingester_stream() {
        assert_eq!(
            FileFormat::for_ingester_stream(StreamType::Metrics, FileFormat::Vortex),
            FileFormat::Parquet
        );
        assert_eq!(
            FileFormat::for_ingester_stream(StreamType::Logs, FileFormat::Vortex),
            FileFormat::Vortex
        );
        assert_eq!(
            FileFormat::for_ingester_stream(StreamType::Traces, FileFormat::Parquet),
            FileFormat::Parquet
        );
    }

    #[test]
    fn test_file_format_from_extension() {
        assert_eq!(
            FileFormat::from_extension("data.parquet"),
            Some(FileFormat::Parquet)
        );
        assert_eq!(
            FileFormat::from_extension("data.vortex"),
            Some(FileFormat::Vortex)
        );
        assert_eq!(FileFormat::from_extension("data.json"), None);
        assert_eq!(FileFormat::from_extension(""), None);
        // core files dispatch as their own format — they must never fall
        // into a parquet default
        assert_eq!(
            FileFormat::from_extension("files/default/logs/s1/2026/07/21/00/1.vix"),
            Some(FileFormat::Vix)
        );
        // full path
        assert_eq!(
            FileFormat::from_extension("/some/path/file.parquet"),
            Some(FileFormat::Parquet)
        );
    }

    #[test]
    fn test_file_format_preserves_configured_value() {
        // Vortex data files are supported in all builds; the configured
        // format is never normalized away.
        let mut cfg = Config::default();
        cfg.common.file_format = FileFormat::Vortex;

        check_file_format_config(&mut cfg);

        assert_eq!(cfg.common.file_format, FileFormat::Vortex);
    }

    #[test]
    fn test_file_format_vix_is_not_a_valid_configured_format() {
        // `vix` parses (needed for internal plumbing) but is normalized away
        // as a ZO_FILE_FORMAT value: logs/traces are always core files, not
        // selected by the flat-data-format switch.
        let mut cfg = Config::default();
        cfg.common.file_format = FileFormat::Vix;

        check_file_format_config(&mut cfg);

        assert_eq!(cfg.common.file_format, FileFormat::Parquet);
    }

    #[test]
    fn test_tls_root_certificates_display() {
        assert_eq!(TlsRootCertificates::Webpki.to_string(), "webpki");
        assert_eq!(TlsRootCertificates::Native.to_string(), "native");
    }

    #[test]
    fn test_tls_root_certificates_from_str() {
        assert_eq!(
            "webpki".parse::<TlsRootCertificates>().unwrap(),
            TlsRootCertificates::Webpki
        );
        assert_eq!(
            "WEBPKI".parse::<TlsRootCertificates>().unwrap(),
            TlsRootCertificates::Webpki
        );
        assert_eq!(
            "native".parse::<TlsRootCertificates>().unwrap(),
            TlsRootCertificates::Native
        );
        assert_eq!(
            "NATIVE".parse::<TlsRootCertificates>().unwrap(),
            TlsRootCertificates::Native
        );
        assert!("invalid".parse::<TlsRootCertificates>().is_err());
    }

    #[test]
    fn test_route_dispatch_strategy_from_str() {
        assert!(matches!(
            "workload".parse::<RouteDispatchStrategy>().unwrap(),
            RouteDispatchStrategy::Workload
        ));
        assert!(matches!(
            "WORKLOAD".parse::<RouteDispatchStrategy>().unwrap(),
            RouteDispatchStrategy::Workload
        ));
        assert!(matches!(
            "random".parse::<RouteDispatchStrategy>().unwrap(),
            RouteDispatchStrategy::Random
        ));
        assert!(matches!(
            "RANDOM".parse::<RouteDispatchStrategy>().unwrap(),
            RouteDispatchStrategy::Random
        ));
        // unknown maps to Other, not an error
        assert!(matches!(
            "unknown".parse::<RouteDispatchStrategy>().unwrap(),
            RouteDispatchStrategy::Other
        ));
        assert!(matches!(
            "  workload  ".parse::<RouteDispatchStrategy>().unwrap(),
            RouteDispatchStrategy::Workload
        ));
    }

    #[test]
    fn test_get_parquet_compression() {
        use parquet::basic::Compression;
        assert_eq!(get_parquet_compression("snappy"), Compression::SNAPPY);
        assert_eq!(
            get_parquet_compression("uncompressed"),
            Compression::UNCOMPRESSED
        );
        assert_eq!(get_parquet_compression("none"), Compression::UNCOMPRESSED);
        assert_eq!(get_parquet_compression("lz4"), Compression::LZ4_RAW);
        assert_eq!(get_parquet_compression("lz4_raw"), Compression::LZ4_RAW);
        assert_eq!(get_parquet_compression("SNAPPY"), Compression::SNAPPY);
        // unknown defaults to zstd
        assert!(matches!(
            get_parquet_compression("unknown"),
            Compression::ZSTD(_)
        ));
        assert!(matches!(
            get_parquet_compression("gzip"),
            Compression::GZIP(_)
        ));
        assert!(matches!(
            get_parquet_compression("brotli"),
            Compression::BROTLI(_)
        ));
        assert!(matches!(
            get_parquet_compression("zstd"),
            Compression::ZSTD(_)
        ));
    }

    #[test]
    fn test_common_should_create_span() {
        let mut common = Common::default();
        assert!(!common.should_create_span());

        common.tracing_enabled = true;
        assert!(common.should_create_span());

        common.tracing_enabled = false;
        common.tracing_search_enabled = true;
        assert!(common.should_create_span());

        common.tracing_search_enabled = false;
        common.search_inspector_enabled = true;
        assert!(common.should_create_span());
    }

    #[test]
    fn test_check_grpc_config_no_tls() {
        let mut cfg = Config::default();
        cfg.grpc.tls_enabled = false;
        assert!(check_grpc_config(&mut cfg).is_ok());
    }

    #[test]
    fn test_check_grpc_config_tls_missing_fields() {
        let mut cfg = Config::default();
        cfg.grpc.tls_enabled = true;
        // All TLS fields empty — should fail
        assert!(check_grpc_config(&mut cfg).is_err());
    }

    #[test]
    fn test_check_grpc_config_tls_complete() {
        let mut cfg = Config::default();
        cfg.grpc.tls_enabled = true;
        cfg.grpc.tls_cert_domain = "example.com".to_string();
        cfg.grpc.tls_cert_path = "/certs/server.crt".to_string();
        cfg.grpc.tls_key_path = "/certs/server.key".to_string();
        assert!(check_grpc_config(&mut cfg).is_ok());
    }

    #[test]
    fn test_check_http_config_no_tls() {
        let mut cfg = Config::default();
        cfg.http.tls_enabled = false;
        assert!(check_http_config(&mut cfg).is_ok());
    }

    #[test]
    fn test_check_http_config_tls_missing_fields() {
        let mut cfg = Config::default();
        cfg.http.tls_enabled = true;
        // Both cert and key empty — should fail
        assert!(check_http_config(&mut cfg).is_err());

        cfg.http.tls_cert_path = "/certs/server.crt".to_string();
        // key still missing — should fail
        assert!(check_http_config(&mut cfg).is_err());
    }

    #[test]
    fn test_check_http_config_tls_complete() {
        let mut cfg = Config::default();
        cfg.http.tls_enabled = true;
        cfg.http.tls_cert_path = "/certs/server.crt".to_string();
        cfg.http.tls_key_path = "/certs/server.key".to_string();
        assert!(check_http_config(&mut cfg).is_ok());
    }

    #[test]
    fn test_check_nats_config_defaults() {
        let mut cfg = Config::default();
        cfg.nats.queue_max_size = 0;
        check_nats_config(&mut cfg).unwrap();
        // 2048 MB → bytes
        assert_eq!(cfg.nats.queue_max_size, 2048 * 1024 * 1024);
    }

    #[test]
    fn test_check_nats_config_custom() {
        let mut cfg = Config::default();
        cfg.nats.queue_max_size = 1;
        check_nats_config(&mut cfg).unwrap();
        assert_eq!(cfg.nats.queue_max_size, 1 * 1024 * 1024);
    }

    #[test]
    fn test_check_limit_config_vix_search_concurrency() {
        // default (0) resolves to 4x cpu cores, capped at 64
        let mut cfg = Config::default();
        cfg.limit.vix_search_concurrency = 0;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.limit.vix_search_concurrency,
            (cfg.limit.cpu_num * 4).min(64)
        );
        // explicit values are respected (floored at 1)
        let mut cfg = Config::default();
        cfg.limit.vix_search_concurrency = 7;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.vix_search_concurrency, 7);
    }

    #[test]
    fn test_check_limit_config_file_move_role_scaling() {
        // dedicated ingester (cluster mode): full core count (no co-located
        // heavy roles to share cores with)
        let mut cfg = Config::default();
        cfg.common.local_mode = false;
        cfg.common.node_role = "ingester".to_string();
        cfg.limit.file_move_thread_num = 0;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.file_move_thread_num, cfg.limit.cpu_num);

        // combined ingester+querier+compactor: divided by 3 so the move,
        // query and merge pools sum to ~cores instead of stacking
        let mut cfg = Config::default();
        cfg.common.local_mode = false;
        cfg.common.node_role = "ingester,querier,compactor".to_string();
        cfg.limit.file_move_thread_num = 0;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.limit.file_move_thread_num,
            std::cmp::max(1, cfg.limit.cpu_num / 3)
        );

        // Role::All in cluster mode counts as all three heavy roles
        let mut cfg = Config::default();
        cfg.common.local_mode = false;
        cfg.common.node_role = "all".to_string();
        cfg.limit.file_move_thread_num = 0;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.limit.file_move_thread_num,
            std::cmp::max(1, cfg.limit.cpu_num / 3)
        );

        // LOCAL_MODE: full core count — single-node behavior unchanged
        let mut cfg = Config::default();
        cfg.common.local_mode = true;
        cfg.common.node_role = "all".to_string();
        cfg.limit.file_move_thread_num = 0;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.file_move_thread_num, cfg.limit.cpu_num);

        // explicit value always respected
        let mut cfg = Config::default();
        cfg.common.local_mode = false;
        cfg.common.node_role = "ingester,querier,compactor".to_string();
        cfg.limit.file_move_thread_num = 5;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.file_move_thread_num, 5);
    }

    #[test]
    fn test_check_inverted_index_config_defaults() {
        let mut cfg = Config::default();
        cfg.common.vix_merge_type_policy.clear();
        cfg.limit.inverted_index_result_cache_max_entries = 0;
        cfg.limit.inverted_index_result_cache_max_entry_size = 0;
        cfg.limit.inverted_index_result_cache_max_size = 0;
        cfg.limit.inverted_index_skip_threshold = 0;
        cfg.limit.inverted_index_min_token_length = 0;
        cfg.limit.inverted_index_max_token_length = 0;
        check_inverted_index_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.inverted_index_result_cache_max_entries, 100000);
        assert_eq!(cfg.limit.inverted_index_result_cache_max_entry_size, 524288);
        assert_eq!(cfg.limit.inverted_index_result_cache_max_size, 256);
        assert_eq!(cfg.limit.inverted_index_skip_threshold, 35);
        assert_eq!(cfg.limit.inverted_index_min_token_length, 2);
        assert_eq!(cfg.limit.inverted_index_max_token_length, 64);
        assert_eq!(cfg.common.vix_merge_type_policy, "legacy");
    }

    #[test]
    fn test_check_inverted_index_config_preserves_existing() {
        let mut cfg = Config::default();
        cfg.common.vix_merge_type_policy = " Latest_Schema ".to_string();
        cfg.limit.inverted_index_result_cache_max_entries = 5000;
        cfg.limit.inverted_index_min_token_length = 3;
        cfg.limit.inverted_index_max_token_length = 32;
        check_inverted_index_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.inverted_index_result_cache_max_entries, 5000);
        assert_eq!(cfg.limit.inverted_index_min_token_length, 3);
        assert_eq!(cfg.limit.inverted_index_max_token_length, 32);
        assert_eq!(cfg.common.vix_merge_type_policy, "latest_schema");
    }

    #[test]
    fn test_check_inverted_index_config_rejects_unknown_merge_type_policy() {
        let mut cfg = Config::default();
        cfg.common.vix_merge_type_policy = "guess".to_string();
        let error = check_inverted_index_config(&mut cfg).unwrap_err();
        assert!(error.to_string().contains("ZO_VIX_MERGE_TYPE_POLICY"));
    }

    #[test]
    fn test_check_health_check_config_defaults() {
        let mut cfg = Config::default();
        cfg.health_check.timeout = 0;
        cfg.health_check.failed_times = 0;
        check_health_check_config(&mut cfg).unwrap();
        assert_eq!(cfg.health_check.timeout, 5);
        assert_eq!(cfg.health_check.failed_times, 3);
    }

    #[test]
    fn test_check_health_check_config_preserves_existing() {
        let mut cfg = Config::default();
        cfg.health_check.timeout = 10;
        cfg.health_check.failed_times = 5;
        check_health_check_config(&mut cfg).unwrap();
        assert_eq!(cfg.health_check.timeout, 10);
        assert_eq!(cfg.health_check.failed_times, 5);
    }

    #[test]
    fn test_check_pipeline_config_defaults() {
        let mut cfg = Config::default();
        cfg.common.data_dir = "/data/".to_string();
        cfg.pipeline.remote_stream_wal_dir = "".to_string();
        cfg.pipeline.offset_flush_interval = 0;
        cfg.pipeline.remote_request_max_retry_time = 0;
        cfg.pipeline.pipeline_file_push_back_interval = 0;
        cfg.pipeline.pipeline_sink_task_spawn_interval_ms = 0;
        check_pipeline_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.pipeline.remote_stream_wal_dir,
            "/data/remote_stream_wal/"
        );
        assert_eq!(cfg.pipeline.offset_flush_interval, 10);
        assert_eq!(cfg.pipeline.remote_request_max_retry_time, 86400);
        assert_eq!(cfg.pipeline.pipeline_file_push_back_interval, 2);
        assert_eq!(cfg.pipeline.pipeline_sink_task_spawn_interval_ms, 100);
    }

    #[test]
    fn test_check_pipeline_config_adds_trailing_slash() {
        let mut cfg = Config::default();
        cfg.common.data_dir = "/data/".to_string();
        cfg.pipeline.remote_stream_wal_dir = "/custom/wal".to_string();
        cfg.pipeline.offset_flush_interval = 5;
        cfg.pipeline.remote_request_max_retry_time = 3600;
        check_pipeline_config(&mut cfg).unwrap();
        assert_eq!(cfg.pipeline.remote_stream_wal_dir, "/custom/wal/");
        assert_eq!(cfg.pipeline.offset_flush_interval, 5);
        assert_eq!(cfg.pipeline.remote_request_max_retry_time, 3600);
    }

    #[test]
    fn test_check_compact_config_defaults() {
        let mut cfg = Config::default();
        cfg.compact.data_retention_days = 0;
        cfg.compact.interval = 0;
        cfg.compact.max_file_size = 0;
        cfg.compact.traces_indexed_max_file_size = 0;
        cfg.compact.delete_files_delay_hours = 0;
        cfg.compact.data_retention_interval = 0;
        cfg.compact.old_data_interval = 0;
        cfg.compact.old_data_max_days = 0;
        cfg.compact.old_data_min_hours = 0;
        cfg.compact.old_data_min_files = 0;
        cfg.compact.file_list_deleted_batch_size = 0;
        cfg.compact.batch_size = 0;
        cfg.compact.pending_jobs_metric_interval = 0;
        check_compact_config(&mut cfg).unwrap();
        assert_eq!(cfg.compact.interval, 10);
        assert_eq!(cfg.compact.max_file_size, 512 * 1024 * 1024);
        assert_eq!(cfg.compact.traces_indexed_max_file_size, 0);
        assert_eq!(
            cfg.compact
                .max_file_size_for_merge(StreamType::Traces, true),
            cfg.compact.max_file_size
        );
        assert_eq!(cfg.compact.delete_files_delay_hours, 2);
        assert_eq!(cfg.compact.data_retention_interval, 3600);
        assert_eq!(cfg.compact.old_data_interval, 3600);
        assert_eq!(cfg.compact.old_data_max_days, 7);
        assert_eq!(cfg.compact.old_data_min_hours, 2);
        assert_eq!(cfg.compact.old_data_min_files, 10);
        assert_eq!(cfg.compact.file_list_deleted_batch_size, 1000);
        assert_eq!(cfg.compact.batch_size, 100);
        assert_eq!(cfg.compact.pending_jobs_metric_interval, 300);
    }

    #[test]
    fn test_trace_indexed_compaction_target_is_class_scoped() {
        let mut cfg = Config::default();
        cfg.compact.max_file_size = 1024;
        cfg.compact.traces_indexed_max_file_size = 4096;
        check_compact_config(&mut cfg).unwrap();

        assert_eq!(
            cfg.compact
                .max_file_size_for_merge(StreamType::Traces, true),
            4096 * 1024 * 1024
        );
        assert_eq!(
            cfg.compact
                .max_file_size_for_merge(StreamType::Traces, false),
            1024 * 1024 * 1024
        );
        assert_eq!(
            cfg.compact.max_file_size_for_merge(StreamType::Logs, true),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_check_compact_config_short_retention_allowed() {
        // M19: engine-owned retention runs at 1 day in the v2 era (S3
        // lifecycle at 2 days is only the backstop) — short values are valid
        let mut cfg = Config::default();
        cfg.compact.data_retention_days = 1;
        assert!(check_compact_config(&mut cfg).is_ok());
        assert_eq!(cfg.compact.data_retention_days, 1);
        cfg.compact.data_retention_days = 2;
        assert!(check_compact_config(&mut cfg).is_ok());
        assert_eq!(cfg.compact.data_retention_days, 2);
    }

    #[test]
    fn test_check_compact_config_valid_retention() {
        let mut cfg = Config::default();
        cfg.compact.data_retention_days = 3;
        assert!(check_compact_config(&mut cfg).is_ok());
        cfg.compact.data_retention_days = 0; // 0 means disabled
        assert!(check_compact_config(&mut cfg).is_ok());
    }

    #[test]
    fn test_check_limit_config_batch_size_clamping() {
        let mut cfg = Config::init().unwrap();
        cfg.limit.batch_size = 0;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.batch_size, 8192);

        cfg.limit.batch_size = 100; // below min
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.batch_size, 1024);

        cfg.limit.batch_size = 10000; // above max
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.batch_size, 8192);

        cfg.limit.batch_size = 4096; // within range
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.batch_size, 4096);
    }

    #[test]
    fn test_check_limit_config_ingest_time_conversion() {
        let mut cfg = Config::init().unwrap();
        cfg.limit.ingest_allowed_upto = 1;
        cfg.limit.ingest_allowed_in_future = 2;
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.ingest_allowed_upto_micro, 1 * 3600 * 1_000_000);
        assert_eq!(
            cfg.limit.ingest_allowed_in_future_micro,
            2 * 3600 * 1_000_000
        );
    }

    /// M20b pin: the ingest window defaults must stay at 5h back / 24h ahead
    /// — the traces span clamp (core::traces::SpanTsClamp) and every logs
    /// ingest path share these limits — and the ZO_INGEST_ALLOWED_UPTO env
    /// override must propagate into the derived micros the clamps consume.
    /// One test covers default + override sequentially: env vars are
    /// process-global, so splitting these into parallel tests would race
    /// (the M12 segment_build_concurrency pin set the pattern).
    #[test]
    fn test_ingest_allowed_upto_default_and_override_m20b() {
        // default: 5h back / 24h ahead
        unsafe { std::env::remove_var("ZO_INGEST_ALLOWED_UPTO") };
        unsafe { std::env::remove_var("ZO_INGEST_ALLOWED_IN_FUTURE") };
        let mut cfg = Config::init().unwrap();
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.ingest_allowed_upto, 5);
        assert_eq!(cfg.limit.ingest_allowed_upto_micro, 5 * 3600 * 1_000_000);
        assert_eq!(cfg.limit.ingest_allowed_in_future, 24);
        assert_eq!(
            cfg.limit.ingest_allowed_in_future_micro,
            24 * 3600 * 1_000_000
        );

        // override wins and propagates into the derived micros
        unsafe { std::env::set_var("ZO_INGEST_ALLOWED_UPTO", "7") };
        let mut cfg = Config::init().unwrap();
        unsafe { std::env::remove_var("ZO_INGEST_ALLOWED_UPTO") };
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.ingest_allowed_upto, 7);
        assert_eq!(cfg.limit.ingest_allowed_upto_micro, 7 * 3600 * 1_000_000);
    }

    #[test]
    fn test_check_limit_config_file_retention_migration() {
        let mut cfg = Config::init().unwrap();
        // deprecated logs_file_retention set to non-hourly should migrate to query_retention
        cfg.limit.logs_file_retention = "daily".to_string();
        cfg.limit.logs_query_retention = "hourly".to_string();
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.logs_query_retention, "daily");
        // file retention always reset to hourly
        assert_eq!(cfg.limit.logs_file_retention, "hourly");
    }

    #[test]
    fn test_check_limit_config_file_retention_no_migration_if_query_set() {
        let mut cfg = Config::init().unwrap();
        // if query_retention was already explicitly set, don't overwrite it
        cfg.limit.logs_file_retention = "daily".to_string();
        cfg.limit.logs_query_retention = "weekly".to_string();
        check_limit_config(&mut cfg).unwrap();
        assert_eq!(cfg.limit.logs_query_retention, "weekly");
    }

    #[test]
    #[allow(deprecated)]
    fn test_check_common_config_bloom_filter_fields_migration() {
        let mut cfg = Config::init().unwrap();
        // deprecated ZO_BLOOM_FILTER_DEFAULT_FIELDS should migrate to the new ENV
        cfg.common.bloom_filter_default_fields = "trace_id,span_id".to_string();
        cfg.common.feature_bloom_filter_extra_fields = "".to_string();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.common.feature_bloom_filter_extra_fields,
            "trace_id,span_id"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_check_common_config_bloom_filter_fields_merge() {
        let mut cfg = Config::init().unwrap();
        // when both ENVs are set, the deprecated one is merged into the new one
        cfg.common.bloom_filter_default_fields = "span_id".to_string();
        cfg.common.feature_bloom_filter_extra_fields = "trace_id".to_string();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(
            cfg.common.feature_bloom_filter_extra_fields,
            "trace_id,span_id"
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_check_common_config_bloom_filter_fields_no_migration() {
        let mut cfg = Config::init().unwrap();
        cfg.common.bloom_filter_default_fields = "".to_string();
        cfg.common.feature_bloom_filter_extra_fields = "trace_id".to_string();
        check_common_config(&mut cfg).unwrap();
        assert_eq!(cfg.common.feature_bloom_filter_extra_fields, "trace_id");
    }

    #[test]
    fn test_check_common_config_segment_knobs() {
        // defaults must pass
        let mut cfg = Config::init().unwrap();
        check_common_config(&mut cfg).unwrap();

        // each knob's floor is enforced with the env var named in the error
        for (set, env) in [
            (
                (|c: &mut Config| c.common.segment_flush_interval_ms = 49) as fn(&mut Config),
                "ZO_SEGMENT_FLUSH_INTERVAL_MS",
            ),
            (
                |c: &mut Config| c.common.segment_flush_size_mb = 0,
                "ZO_SEGMENT_FLUSH_SIZE_MB",
            ),
            (
                |c: &mut Config| c.common.segment_buffer_max_mb = 63,
                "ZO_SEGMENT_BUFFER_MAX_MB",
            ),
            (
                |c: &mut Config| c.common.segment_build_batch = 0,
                "ZO_SEGMENT_BUILD_BATCH",
            ),
            (
                |c: &mut Config| c.common.segment_build_lease_secs = 29,
                "ZO_SEGMENT_BUILD_LEASE_SECS",
            ),
            (
                |c: &mut Config| c.common.segment_retain_secs = 59,
                "ZO_SEGMENT_RETAIN_SECS",
            ),
        ] {
            let mut cfg = Config::init().unwrap();
            set(&mut cfg);
            let err = check_common_config(&mut cfg).unwrap_err().to_string();
            assert!(err.contains(env), "expected {env} in error: {err}");
        }

        // the buffer cap floor scales with the flush size
        let mut cfg = Config::init().unwrap();
        cfg.common.segment_flush_size_mb = 32;
        cfg.common.segment_buffer_max_mb = 64;
        check_common_config(&mut cfg).unwrap();
        cfg.common.segment_buffer_max_mb = 63;
        assert!(check_common_config(&mut cfg).is_err());
    }

    #[test]
    fn test_check_s3_config_bucket_prefix_trailing_slash() {
        let mut cfg = Config::default();
        cfg.s3.server_url = "".to_string();
        cfg.s3.provider = "aws".to_string();
        cfg.s3.bucket_prefix = "prefix1,prefix2".to_string();
        check_s3_config(&mut cfg).unwrap();
        // each prefix should end with /
        assert_eq!(cfg.s3.bucket_prefix, "prefix1/,prefix2/");
    }

    #[test]
    fn test_check_s3_config_bucket_prefix_already_has_slash() {
        let mut cfg = Config::default();
        cfg.s3.provider = "aws".to_string();
        cfg.s3.bucket_prefix = "prefix1/,prefix2/".to_string();
        check_s3_config(&mut cfg).unwrap();
        assert_eq!(cfg.s3.bucket_prefix, "prefix1/,prefix2/");
    }

    #[test]
    fn test_check_s3_config_provider_lowercase() {
        let mut cfg = Config::default();
        cfg.s3.provider = "AWS".to_string();
        check_s3_config(&mut cfg).unwrap();
        assert_eq!(cfg.s3.provider, "aws");
    }

    #[test]
    fn test_check_s3_config_keepalive_default() {
        let mut cfg = Config::default();
        cfg.s3.provider = "aws".to_string();
        cfg.s3.keepalive_timeout = 0;
        check_s3_config(&mut cfg).unwrap();
        assert_eq!(cfg.s3.keepalive_timeout, 20);
    }

    #[test]
    fn test_ensure_not_empty_whitespace_only() {
        let result = ensure_not_empty("   ", "field");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("field"));
    }

    #[test]
    fn test_ensure_not_empty_tab_only() {
        assert!(ensure_not_empty("\t", "field").is_err());
        assert!(ensure_not_empty("\n", "field").is_err());
    }

    #[test]
    fn test_get_batch_size_positive() {
        let size = get_batch_size();
        assert!(size > 0, "batch size should be positive");
    }

    #[test]
    fn test_cache_and_get_instance_id() {
        cache_instance_id("test-instance-abc");
        assert_eq!(get_instance_id(), "test-instance-abc");
        cache_instance_id("test-instance-xyz");
        assert_eq!(get_instance_id(), "test-instance-xyz");
    }

    #[test]
    fn test_get_instance_id_empty_when_not_set() {
        let id = get_instance_id();
        let _ = id.len();
    }

    #[test]
    fn test_is_local_disk_storage_returns_bool() {
        let result: bool = is_local_disk_storage();
        let _ = result;
    }

    #[test]
    fn test_get_cluster_name_returns_nonempty() {
        let name = get_cluster_name();
        assert!(!name.is_empty(), "cluster name should not be empty");
    }

    #[test]
    fn test_deverbatim_plain_path_unchanged() {
        let p = std::path::Path::new("/data/openobserve");
        let result = deverbatim(p);
        assert_eq!(result, "/data/openobserve");
    }

    #[test]
    fn test_deverbatim_empty_path_unchanged() {
        let p = std::path::Path::new("");
        let result = deverbatim(p);
        assert_eq!(result, "");
    }

    #[cfg(windows)]
    #[test]
    fn test_deverbatim_verbatim_disk_stripped() {
        let p = std::path::Path::new(r"\\?\C:\data\openobserve");
        let result = deverbatim(p);
        assert_eq!(result, r"C:\data\openobserve");
    }

    #[cfg(windows)]
    #[test]
    fn test_deverbatim_plain_windows_path_unchanged() {
        let p = std::path::Path::new(r"C:\data\openobserve");
        let result = deverbatim(p);
        assert_eq!(result, r"C:\data\openobserve");
    }
}
