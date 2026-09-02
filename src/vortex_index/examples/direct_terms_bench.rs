//! Local-only benchmark for deriving exact value/FTS postings directly from
//! docs-column dictionaries. It never writes an index or changes production.
//!
//! Usage:
//!   cargo run -p vortex_index --release --example direct_terms_bench -- \
//!     --dict-csr [--verify|--verify-all|--verify=FIELD,...] FILE.vix
//!
//! `--verify` checks a deterministic bounded set (up to four FTS fields, then
//! raw fields, at most eight total). Verification materializes only one field
//! at a time and compares every token and every posting, not just totals.

use std::{
    borrow::Cow,
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    mem::size_of,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow, bail, ensure};
use arrow::array::{
    Array, BooleanArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use bytes::Bytes;
use futures::future::BoxFuture;
use vortex_index::{
    DocsDictChunk, ID_COL_NAME, ORIGINAL_DATA_COL_NAME, SOURCE_COL_NAME, TIMESTAMP_COL_NAME,
    VixRangeSource, VixReader, canonical_bool_text, canonical_f32_text, canonical_f64_text,
    canonical_i64_text, canonical_u64_text, numeric_value_token, o2_tokenize,
};

const RAW_MAX: usize = 65_532;
const FTS_MIN: usize = 2;
const FTS_MAX: usize = 64;
const VERIFY_CAP: usize = 8;
const VERIFY_FTS_CAP: usize = 4;
const SLOW_CAP: usize = 20;

struct FileRangeSource {
    name: String,
    file: std::fs::File,
    len: u64,
}

impl VixRangeSource for FileRangeSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn fetch(&self, range: Range<u64>) -> BoxFuture<'static, anyhow::Result<Bytes>> {
        use std::os::unix::fs::FileExt;
        let result = (|| {
            ensure!(
                range.start <= range.end && range.end <= self.len,
                "read {} phase range_fetch: {}..{} is outside 0..{}",
                self.name,
                range.start,
                range.end,
                self.len
            );
            let len: usize = (range.end - range.start)
                .try_into()
                .context("range length does not fit usize")?;
            let mut bytes = vec![0; len];
            self.file
                .read_exact_at(&mut bytes, range.start)
                .with_context(|| {
                    format!(
                        "read {} phase range_fetch: {}..{}",
                        self.name, range.start, range.end
                    )
                })?;
            Ok(Bytes::from(bytes))
        })();
        Box::pin(futures::future::ready(result))
    }

    fn describe(&self) -> String {
        self.name.clone()
    }
}

fn source(path: &Path) -> anyhow::Result<Arc<dyn VixRangeSource>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("phase open: open {}", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("phase open: stat {}", path.display()))?
        .len();
    Ok(Arc::new(FileRangeSource {
        name: path.display().to_string(),
        file,
        len,
    }))
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Raw,
    Fts,
    KeyOnly,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Fts => "fts",
            Self::KeyOnly => "key_only",
        }
    }

    fn has_values(self) -> bool {
        self != Self::KeyOnly
    }
}

enum Verify {
    None,
    Bounded,
    All,
    Explicit(Vec<String>),
}

struct Options {
    path: PathBuf,
    verify: Verify,
}

fn usage() {
    println!(
        "usage: direct_terms_bench --dict-csr [--verify|--verify-all|--verify=FIELD,...] FILE.vix"
    );
    println!("  --dict-csr       run dictionary-ordinal plus field-local CSR construction");
    println!(
        "  --verify         row-verify a deterministic selection of at most {VERIFY_CAP} fields"
    );
    println!("  --verify-all     row-verify every candidate, still one field at a time");
    println!("  --verify=LIST    row-verify exactly the comma-separated field names");
}

fn set_verify(current: &mut Verify, next: Verify) -> anyhow::Result<()> {
    ensure!(
        matches!(current, Verify::None),
        "choose only one verification option"
    );
    *current = next;
    Ok(())
}

fn options() -> anyhow::Result<Option<Options>> {
    let mut dict_csr = false;
    let mut verify = Verify::None;
    let mut path = None;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-h" | "--help" => return Ok(None),
            "--dict-csr" => dict_csr = true,
            "--verify" => set_verify(&mut verify, Verify::Bounded)?,
            "--verify-all" => set_verify(&mut verify, Verify::All)?,
            _ if arg.starts_with("--verify=") => {
                let names: Vec<String> = arg["--verify=".len()..]
                    .split(',')
                    .filter(|name| !name.is_empty())
                    .map(str::to_owned)
                    .collect();
                ensure!(!names.is_empty(), "--verify= requires at least one field");
                set_verify(&mut verify, Verify::Explicit(names))?;
            }
            _ if arg.starts_with('-') => bail!("unknown option {arg:?}"),
            _ => {
                ensure!(path.is_none(), "expected exactly one FILE.vix argument");
                path = Some(PathBuf::from(arg));
            }
        }
    }
    ensure!(dict_csr, "--dict-csr is required");
    let path = path.context("missing FILE.vix argument")?;
    ensure!(
        path.extension().is_some_and(|extension| extension == "vix"),
        "input must have the .vix extension"
    );
    Ok(Some(Options { path, verify }))
}

/// One owned byte buffer per globally distinct `(field, token)`. Separate
/// field maps permit borrowed `[u8]` lookups, so duplicate dictionary values
/// are never copied into the retained global table.
struct GlobalTerms {
    fields: Vec<HashMap<Vec<u8>, u32>>,
    next: u32,
    bytes: usize,
}

impl GlobalTerms {
    fn new(fields: usize) -> Self {
        Self {
            fields: (0..fields).map(|_| HashMap::new()).collect(),
            next: 0,
            bytes: 0,
        }
    }

    fn intern(&mut self, field: usize, token: Cow<'_, [u8]>) -> anyhow::Result<u32> {
        if let Some(id) = self.fields[field].get(token.as_ref()) {
            return Ok(*id);
        }
        let id = self.next;
        self.next = self
            .next
            .checked_add(1)
            .context("phase dictionary_translate: global term id overflow")?;
        self.bytes = self
            .bytes
            .checked_add(token.len())
            .context("phase dictionary_translate: retained term bytes overflow")?;
        self.fields[field].insert(token.into_owned(), id);
        Ok(id)
    }

    fn count(&self) -> usize {
        self.next as usize
    }

    fn logical_bytes(&self) -> usize {
        self.bytes
            .saturating_add(self.count().saturating_mul(size_of::<u32>()))
    }
}

struct Translation {
    ids: Vec<Vec<usize>>,
    present: Vec<bool>,
    used: Vec<bool>,
}

struct Candidate {
    local: HashMap<u32, usize>,
    offsets: Vec<usize>,
    postings: Vec<u32>,
    slots: usize,
    used_slots: usize,
    present_cells: u64,
    digest: u64,
    decode: Duration,
    translate: Duration,
    csr: Duration,
    logical_bytes: usize,
}

struct KeyScan {
    slots: usize,
    used_slots: usize,
    present_cells: u64,
    decode: Duration,
    validity: Duration,
    logical_bytes: usize,
}

#[derive(Clone)]
struct FieldMetric {
    name: String,
    mode: Mode,
    decode_ns: u128,
    translate_ns: u128,
    construct_ns: u128,
    verify_ns: u128,
    slots: usize,
    used_slots: usize,
    terms: usize,
    postings: usize,
    present_cells: u64,
    dense: bool,
    digest: u64,
}

impl FieldMetric {
    fn total_ns(&self) -> u128 {
        self.decode_ns + self.translate_ns + self.construct_ns + self.verify_ns
    }
}

#[derive(Default)]
struct Totals {
    decode: Duration,
    translate: Duration,
    csr: Duration,
    validity: Duration,
    verify: Duration,
}

#[derive(Clone, Copy)]
struct Digest(u64);

impl Digest {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.u64(bytes.len() as u64);
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

fn internal(name: &str) -> bool {
    name == TIMESTAMP_COL_NAME
        || name == ID_COL_NAME
        || name == ORIGINAL_DATA_COL_NAME
        || name == SOURCE_COL_NAME
}

fn typed<T: Array + 'static>(array: &dyn Array) -> anyhow::Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow!("array downcast failed for {:?}", array.data_type()))
}

fn string_at(array: &dyn Array, row: usize) -> anyhow::Result<Option<&str>> {
    if !array.is_valid(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(typed::<StringArray>(array)?.value(row))),
        DataType::LargeUtf8 => Ok(Some(typed::<LargeStringArray>(array)?.value(row))),
        DataType::Utf8View => Ok(Some(typed::<StringViewArray>(array)?.value(row))),
        _ => Ok(None),
    }
}

/// Exact current writer semantics for a single value. FTS tokens are sorted
/// and deduplicated here, once per dictionary slot, before ordinal translation.
fn tokens<'a>(array: &'a dyn Array, row: usize, mode: Mode) -> anyhow::Result<Vec<Cow<'a, [u8]>>> {
    if !array.is_valid(row) {
        return Ok(Vec::new());
    }
    if let Some(value) = string_at(array, row)? {
        if mode == Mode::Fts {
            let mut out: Vec<Cow<'a, [u8]>> = o2_tokenize(value, FTS_MIN, FTS_MAX)
                .map(|token| Cow::Owned(token.into_bytes()))
                .collect();
            out.sort_unstable_by(|left, right| left.as_ref().cmp(right.as_ref()));
            out.dedup_by(|left, right| left.as_ref() == right.as_ref());
            return Ok(out);
        }
        if value.len() > RAW_MAX {
            return Ok(Vec::new());
        }
        return Ok(vec![Cow::Borrowed(value.as_bytes())]);
    }
    if mode == Mode::Fts {
        return Ok(Vec::new());
    }
    let text = match array.data_type() {
        DataType::Boolean => {
            Some(canonical_bool_text(typed::<BooleanArray>(array)?.value(row)).to_owned())
        }
        DataType::Int8 => Some(canonical_i64_text(i64::from(
            typed::<Int8Array>(array)?.value(row),
        ))),
        DataType::Int16 => Some(canonical_i64_text(i64::from(
            typed::<Int16Array>(array)?.value(row),
        ))),
        DataType::Int32 => Some(canonical_i64_text(i64::from(
            typed::<Int32Array>(array)?.value(row),
        ))),
        DataType::Int64 => Some(canonical_i64_text(typed::<Int64Array>(array)?.value(row))),
        DataType::UInt8 => Some(canonical_u64_text(u64::from(
            typed::<UInt8Array>(array)?.value(row),
        ))),
        DataType::UInt16 => Some(canonical_u64_text(u64::from(
            typed::<UInt16Array>(array)?.value(row),
        ))),
        DataType::UInt32 => Some(canonical_u64_text(u64::from(
            typed::<UInt32Array>(array)?.value(row),
        ))),
        DataType::UInt64 => Some(canonical_u64_text(typed::<UInt64Array>(array)?.value(row))),
        DataType::Float16 => canonical_f32_text(typed::<Float16Array>(array)?.value(row).to_f32()),
        DataType::Float32 => canonical_f32_text(typed::<Float32Array>(array)?.value(row)),
        DataType::Float64 => canonical_f64_text(typed::<Float64Array>(array)?.value(row)),
        data_type => bail!("raw value terms are unsupported for {data_type:?}"),
    };
    Ok(text
        .map(|text| vec![Cow::Owned(numeric_value_token(&text))])
        .unwrap_or_default())
}

fn slot_present(array: &dyn Array, row: usize) -> anyhow::Result<bool> {
    if !array.is_valid(row) {
        return Ok(false);
    }
    match array.data_type() {
        DataType::Float16 => Ok(typed::<Float16Array>(array)?
            .value(row)
            .to_f32()
            .is_finite()),
        DataType::Float32 => Ok(typed::<Float32Array>(array)?.value(row).is_finite()),
        DataType::Float64 => Ok(typed::<Float64Array>(array)?.value(row).is_finite()),
        _ => Ok(true),
    }
}

fn code(
    chunk: &DocsDictChunk,
    row: usize,
    field: &str,
    phase: &str,
) -> anyhow::Result<Option<usize>> {
    if !chunk.codes.is_valid(row) {
        return Ok(None);
    }
    let code: usize = chunk.codes.value(row).try_into().with_context(|| {
        format!("field {field:?} phase {phase}: dictionary code does not fit usize")
    })?;
    ensure!(
        code < chunk.values.len(),
        "field {field:?} phase {phase}: code {code} exceeds {} slots",
        chunk.values.len()
    );
    Ok(Some(code))
}

fn chunk_bytes(chunks: &[DocsDictChunk]) -> usize {
    chunks.iter().fold(0usize, |sum, chunk| {
        sum.saturating_add(chunk.codes.get_array_memory_size())
            .saturating_add(chunk.values.get_array_memory_size())
    })
}

fn candidate(
    reader: &VixReader,
    field_index: usize,
    field: &str,
    mode: Mode,
    rows: usize,
    global: &mut GlobalTerms,
) -> anyhow::Result<Candidate> {
    let started = Instant::now();
    let chunks = reader
        .read_docs_column_dict(field)
        .with_context(|| format!("field {field:?} phase dictionary_decode"))?;
    let decode = started.elapsed();

    let started = Instant::now();
    let mut slots = 0usize;
    let mut decoded_rows = 0usize;
    let mut local = HashMap::<u32, usize>::new();
    let mut translated = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        slots = slots.checked_add(chunk.values.len()).with_context(|| {
            format!("field {field:?} phase dictionary_translate: slot overflow")
        })?;
        let mut used = vec![false; chunk.values.len()];
        for row in 0..chunk.codes.len() {
            if let Some(slot) = code(chunk, row, field, "dictionary_translate")? {
                used[slot] = true;
            }
            decoded_rows += 1;
        }
        let mut ids = Vec::with_capacity(chunk.values.len());
        let mut present = Vec::with_capacity(chunk.values.len());
        for slot in 0..chunk.values.len() {
            let derived = tokens(chunk.values.as_ref(), slot, mode).with_context(|| {
                format!("field {field:?} phase dictionary_translate slot={slot}")
            })?;
            let mut slot_ids = Vec::with_capacity(derived.len());
            if used[slot] {
                for token in derived {
                    let global_id = global
                        .intern(field_index, token)
                        .with_context(|| format!("field {field:?} phase dictionary_translate"))?;
                    let next = local.len();
                    slot_ids.push(*local.entry(global_id).or_insert(next));
                }
                slot_ids.sort_unstable();
                slot_ids.dedup();
            }
            ids.push(slot_ids);
            present.push(
                slot_present(chunk.values.as_ref(), slot)
                    .with_context(|| format!("field {field:?} phase key_validity slot={slot}"))?,
            );
        }
        translated.push(Translation { ids, present, used });
    }
    ensure!(
        decoded_rows == rows,
        "field {field:?} phase dictionary_translate: decoded {decoded_rows} rows, expected {rows}"
    );
    let translate = started.elapsed();

    let started = Instant::now();
    let mut counts = vec![0usize; local.len()];
    let mut present_cells = 0u64;
    let mut document = 0usize;
    for (chunk, translated) in chunks.iter().zip(&translated) {
        for row in 0..chunk.codes.len() {
            if let Some(slot) = code(chunk, row, field, "csr_count")? {
                if translated.present[slot] {
                    present_cells = present_cells.checked_add(1).with_context(|| {
                        format!("field {field:?} phase key_validity: count overflow")
                    })?;
                }
                for id in &translated.ids[slot] {
                    counts[*id] = counts[*id].checked_add(1).with_context(|| {
                        format!("field {field:?} phase csr_count: posting count overflow")
                    })?;
                }
            }
            document += 1;
        }
    }
    ensure!(
        document == rows,
        "field {field:?} phase csr_count: row mismatch"
    );

    let mut offsets = Vec::with_capacity(counts.len() + 1);
    offsets.push(0usize);
    for count in &counts {
        let next = offsets[offsets.len() - 1]
            .checked_add(*count)
            .with_context(|| format!("field {field:?} phase csr_count: offset overflow"))?;
        offsets.push(next);
    }
    let mut cursors = offsets[..counts.len()].to_vec();
    let mut postings = vec![0u32; offsets.last().copied().unwrap_or(0)];
    document = 0;
    for (chunk, translated) in chunks.iter().zip(&translated) {
        for row in 0..chunk.codes.len() {
            if let Some(slot) = code(chunk, row, field, "csr_fill")? {
                let doc_id: u32 = document.try_into().with_context(|| {
                    format!("field {field:?} phase csr_fill: doc id exceeds u32")
                })?;
                for id in &translated.ids[slot] {
                    postings[cursors[*id]] = doc_id;
                    cursors[*id] += 1;
                }
            }
            document += 1;
        }
    }
    ensure!(
        cursors
            .iter()
            .zip(offsets.iter().skip(1))
            .all(|(actual, expected)| actual == expected),
        "field {field:?} phase csr_fill: cursor/count mismatch"
    );
    for id in 0..counts.len() {
        let list = &mut postings[offsets[id]..offsets[id + 1]];
        list.sort_unstable();
        ensure!(
            list.windows(2).all(|pair| pair[0] < pair[1]),
            "field {field:?} phase csr_sort: duplicate posting"
        );
    }

    let mut ordered = Vec::with_capacity(local.len());
    for (token, global_id) in &global.fields[field_index] {
        let local_id = local
            .get(global_id)
            .copied()
            .with_context(|| format!("field {field:?} phase digest: global/local map mismatch"))?;
        ordered.push((token.as_slice(), local_id));
    }
    ensure!(
        ordered.len() == local.len(),
        "field {field:?} phase digest: term count mismatch"
    );
    ordered.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut digest = Digest::new();
    for (token, id) in ordered {
        let list = &postings[offsets[id]..offsets[id + 1]];
        digest.bytes(token);
        digest.u64(list.len() as u64);
        for doc in list {
            digest.u64(u64::from(*doc));
        }
    }
    let digest = digest.finish();
    let csr = started.elapsed();

    let used_slots = translated
        .iter()
        .map(|chunk| chunk.used.iter().filter(|used| **used).count())
        .sum();
    let translation_bytes = translated.iter().fold(0usize, |sum, chunk| {
        sum.saturating_add(chunk.ids.len().saturating_mul(size_of::<Vec<usize>>()))
            .saturating_add(
                chunk
                    .ids
                    .iter()
                    .map(|ids| ids.len().saturating_mul(size_of::<usize>()))
                    .sum::<usize>(),
            )
            .saturating_add(chunk.present.len().saturating_mul(size_of::<bool>()))
            .saturating_add(chunk.used.len().saturating_mul(size_of::<bool>()))
    });
    let logical_bytes = global
        .logical_bytes()
        .saturating_add(chunk_bytes(&chunks))
        .saturating_add(translation_bytes)
        .saturating_add(
            local
                .len()
                .saturating_mul(size_of::<u32>() + size_of::<usize>()),
        )
        .saturating_add(counts.len().saturating_mul(size_of::<usize>()))
        .saturating_add(offsets.len().saturating_mul(size_of::<usize>()))
        .saturating_add(cursors.len().saturating_mul(size_of::<usize>()))
        .saturating_add(postings.len().saturating_mul(size_of::<u32>()));

    Ok(Candidate {
        local,
        offsets,
        postings,
        slots,
        used_slots,
        present_cells,
        digest,
        decode,
        translate,
        csr,
        logical_bytes,
    })
}

fn key_scan(
    reader: &VixReader,
    field: &str,
    rows: usize,
    retained: usize,
) -> anyhow::Result<KeyScan> {
    let started = Instant::now();
    let chunks = reader
        .read_docs_column_dict(field)
        .with_context(|| format!("field {field:?} phase key_dictionary_decode"))?;
    let decode = started.elapsed();
    let started = Instant::now();
    let base = retained.saturating_add(chunk_bytes(&chunks));
    let mut logical_bytes = base;
    let mut slots = 0usize;
    let mut used_slots = 0usize;
    let mut present_cells = 0u64;
    let mut documents = 0usize;
    for chunk in &chunks {
        slots = slots
            .checked_add(chunk.values.len())
            .with_context(|| format!("field {field:?} phase key_validity: slot overflow"))?;
        let mut present = Vec::with_capacity(chunk.values.len());
        for slot in 0..chunk.values.len() {
            present.push(
                slot_present(chunk.values.as_ref(), slot)
                    .with_context(|| format!("field {field:?} phase key_validity slot={slot}"))?,
            );
        }
        let mut used = vec![false; chunk.values.len()];
        for row in 0..chunk.codes.len() {
            if let Some(slot) = code(chunk, row, field, "key_validity")? {
                used[slot] = true;
                if present[slot] {
                    present_cells = present_cells.checked_add(1).with_context(|| {
                        format!("field {field:?} phase key_validity: count overflow")
                    })?;
                }
            }
            documents += 1;
        }
        used_slots = used_slots
            .checked_add(used.iter().filter(|used| **used).count())
            .with_context(|| format!("field {field:?} phase key_validity: used-slot overflow"))?;
        logical_bytes = logical_bytes.max(
            base.saturating_add(present.len().saturating_mul(size_of::<bool>()))
                .saturating_add(used.len().saturating_mul(size_of::<bool>())),
        );
    }
    ensure!(
        documents == rows,
        "field {field:?} phase key_validity: decoded {documents} rows, expected {rows}"
    );
    Ok(KeyScan {
        slots,
        used_slots,
        present_cells,
        decode,
        validity: started.elapsed(),
        logical_bytes,
    })
}

fn verify_field(
    reader: &VixReader,
    field_index: usize,
    field: &str,
    mode: Mode,
    rows: usize,
    global: &GlobalTerms,
    candidate: &Candidate,
) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let values = reader
        .read_docs_column(field)
        .with_context(|| format!("field {field:?} phase row_verify_decode"))?;
    ensure!(
        values.len() == rows,
        "field {field:?} phase row_verify: decoded {} rows, expected {rows}",
        values.len()
    );
    let mut reference = BTreeMap::<Vec<u8>, Vec<u32>>::new();
    for row in 0..values.len() {
        let doc: u32 = row
            .try_into()
            .with_context(|| format!("field {field:?} phase row_verify: doc id exceeds u32"))?;
        for token in tokens(values.as_ref(), row, mode)
            .with_context(|| format!("field {field:?} phase row_verify row={row}"))?
        {
            reference.entry(token.into_owned()).or_default().push(doc);
        }
    }
    ensure!(
        reference.len() == candidate.local.len(),
        "field {field:?} phase row_verify: token count mismatch candidate={} reference={}",
        candidate.local.len(),
        reference.len()
    );
    let mut ordered: Vec<(&[u8], usize)> = global.fields[field_index]
        .iter()
        .map(|(token, global_id)| {
            candidate
                .local
                .get(global_id)
                .copied()
                .map(|local| (token.as_slice(), local))
                .with_context(|| {
                    format!("field {field:?} phase row_verify: global/local map mismatch")
                })
        })
        .collect::<anyhow::Result<_>>()?;
    ordered.sort_unstable_by(|left, right| left.0.cmp(right.0));
    for (ordinal, (token, local)) in ordered.into_iter().enumerate() {
        let expected = reference.get(token).with_context(|| {
            format!("field {field:?} phase row_verify: token mismatch ordinal={ordinal}")
        })?;
        let actual = &candidate.postings[candidate.offsets[local]..candidate.offsets[local + 1]];
        ensure!(
            actual == expected.as_slice(),
            "field {field:?} phase row_verify: postings mismatch ordinal={ordinal} candidate_count={} reference_count={}",
            actual.len(),
            expected.len()
        );
    }
    Ok(started.elapsed())
}

fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

fn verification(
    verify: &Verify,
    fields: &[(usize, String, DataType, Mode)],
) -> anyhow::Result<(BTreeSet<String>, &'static str)> {
    let candidates: BTreeSet<&str> = fields
        .iter()
        .filter(|(_, _, _, mode)| mode.has_values())
        .map(|(_, name, ..)| name.as_str())
        .collect();
    match verify {
        Verify::None => Ok((BTreeSet::new(), "none")),
        Verify::All => Ok((candidates.into_iter().map(str::to_owned).collect(), "all")),
        Verify::Explicit(names) => {
            let selected: BTreeSet<String> = names.iter().cloned().collect();
            for name in &selected {
                ensure!(
                    candidates.contains(name.as_str()),
                    "field {name:?} phase verification_scope: not a raw/FTS candidate"
                );
            }
            Ok((selected, "explicit"))
        }
        Verify::Bounded => {
            let mut selected = BTreeSet::new();
            for (_, name, ..) in fields
                .iter()
                .filter(|(_, _, _, mode)| *mode == Mode::Fts)
                .take(VERIFY_FTS_CAP)
            {
                selected.insert(name.clone());
            }
            for (_, name, ..) in fields.iter().filter(|(_, _, _, mode)| *mode == Mode::Raw) {
                if selected.len() == VERIFY_CAP {
                    break;
                }
                selected.insert(name.clone());
            }
            Ok((selected, "bounded"))
        }
    }
}

fn run(options: Options) -> anyhow::Result<()> {
    let wall_started = Instant::now();
    let index_path = options.path.with_extension("vxi");
    ensure!(
        index_path.is_file(),
        "phase open: sibling .vxi is required at {}",
        index_path.display()
    );
    let started = Instant::now();
    let reader =
        VixReader::open_ranged_with_index(source(&options.path)?, Some(source(&index_path)?))
            .context("phase open: VixReader")?;
    ensure!(
        reader.has_index(),
        "phase open: sidecar did not provide an index"
    );
    let schema = reader.docs_schema().context("phase open: docs schema")?;
    let rows: usize = reader
        .row_count()
        .try_into()
        .context("phase open: row count does not fit usize")?;
    ensure!(
        reader.row_count() <= u64::from(u32::MAX),
        "phase open: row count exceeds u32 posting space"
    );
    let term_names: HashSet<String> = reader
        .term_fields()
        .map(|(_, name)| name.to_owned())
        .collect();
    let fts_names = reader.fts_fields().clone();
    let bloom_names: HashSet<String> = reader.bloom_only_fields().map(str::to_owned).collect();
    let open = started.elapsed();

    let schema_names: HashSet<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    for name in term_names.iter().chain(fts_names.iter()) {
        ensure!(
            schema_names.contains(name.as_str()),
            "field {name:?} phase schema_role: indexed field absent from docs schema"
        );
    }
    let mut fields = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if internal(field.name()) {
            continue;
        }
        let mode = if bloom_names.contains(field.name()) {
            Mode::KeyOnly
        } else if fts_names.contains(field.name()) {
            Mode::Fts
        } else if term_names.contains(field.name()) {
            Mode::Raw
        } else {
            Mode::KeyOnly
        };
        fields.push((index, field.name().clone(), field.data_type().clone(), mode));
    }
    let raw = fields
        .iter()
        .filter(|(_, _, _, mode)| *mode == Mode::Raw)
        .count();
    let fts = fields
        .iter()
        .filter(|(_, _, _, mode)| *mode == Mode::Fts)
        .count();
    let (verify, verify_scope) = verification(&options.verify, &fields)?;

    println!("run mode=dict-csr output=measurement_only on_disk_write=false");
    println!(
        "scope rows={} schema_fields={} raw_fields={} fts_fields={} bloom_only_fields={} key_fields={} candidate_fields={} source_read=false final_doc_order=stored single_concat_input=true source_row_order={:?}",
        rows,
        schema.fields().len(),
        raw,
        fts,
        bloom_names.len(),
        fields.len(),
        raw + fts,
        reader.row_order()
    );
    println!(
        "semantics raw_empty=emit raw_max_bytes={RAW_MAX} raw_oversize=skip fts_min_bytes={FTS_MIN} fts_max_bytes_exclusive={FTS_MAX} fts_raw_limit=ignored same_doc_duplicates=dedup numeric_prefix=0x01 null=absent nonfinite_float=absent bloom_only_value_terms=absent"
    );
    println!(
        "semantics key_exclusions=_timestamp%2C_o2_id%2C_original%2C_source dictionaries=chunk_local duplicate_slots=translated_once null_slots=translated_to_empty unreferenced_slots=translated_but_not_emitted global_term_bytes=one_copy field_working_set=one_field"
    );
    println!(
        "limitations local_files_only=true production_integration=false format_change=false existing_sidecar_comparison=false whole_index_parity_claim=false candidate_key_validity_timing=inside_csr key_only_validity_timing=separate timings_include_local_io_and_cache_state=true peak_logical_excludes=allocator_overhead%2Chash_buckets%2Creader_caches%2Cverifier checksum_exposes_values=false"
    );
    println!(
        "verification_scope mode={} selected_fields={} bounded_cap={VERIFY_CAP} reference=read_docs_column compare=token_bytes_and_exact_postings source_read=false",
        verify_scope,
        verify.len()
    );
    for field in &verify {
        println!("verification_field field={}", escape(field));
    }
    println!("phase scope=file phase=open ns={}", open.as_nanos());

    let mut global = GlobalTerms::new(schema.fields().len());
    let mut totals = Totals::default();
    let mut metrics = Vec::with_capacity(fields.len());
    let mut peak = 0usize;
    let mut all_slots = 0usize;
    let mut all_used_slots = 0usize;
    let mut all_postings = 0usize;
    let mut all_present = 0u64;
    let mut dense_keys = 0usize;
    let mut checksum = Digest::new();

    for (field_index, field, _, mode) in fields {
        if mode.has_values() {
            let result = candidate(&reader, field_index, &field, mode, rows, &mut global)?;
            let verify_time = if verify.contains(&field) {
                verify_field(&reader, field_index, &field, mode, rows, &global, &result)?
            } else {
                Duration::ZERO
            };
            totals.decode += result.decode;
            totals.translate += result.translate;
            totals.csr += result.csr;
            totals.verify += verify_time;
            peak = peak.max(result.logical_bytes);
            all_slots += result.slots;
            all_used_slots += result.used_slots;
            all_postings += result.postings.len();
            all_present += result.present_cells;
            let dense = rows > 0 && result.present_cells == rows as u64;
            dense_keys += usize::from(dense);
            checksum.bytes(field.as_bytes());
            checksum.u64(result.digest);
            checksum.u64(result.present_cells);
            let metric = FieldMetric {
                name: field,
                mode,
                decode_ns: result.decode.as_nanos(),
                translate_ns: result.translate.as_nanos(),
                construct_ns: result.csr.as_nanos(),
                verify_ns: verify_time.as_nanos(),
                slots: result.slots,
                used_slots: result.used_slots,
                terms: result.local.len(),
                postings: result.postings.len(),
                present_cells: result.present_cells,
                dense,
                digest: result.digest,
            };
            println!(
                "field scope=candidate field={} mode={} verified={} dict_decode_ns={} dict_translate_ns={} csr_count_fill_sort_digest_ns={} row_verify_ns={} dictionary_slots={} used_slots={} value_terms={} postings={} key_present_cells={} dense_key={} checksum={:016x}",
                escape(&metric.name),
                metric.mode.label(),
                verify.contains(&metric.name),
                metric.decode_ns,
                metric.translate_ns,
                metric.construct_ns,
                metric.verify_ns,
                metric.slots,
                metric.used_slots,
                metric.terms,
                metric.postings,
                metric.present_cells,
                metric.dense,
                metric.digest
            );
            metrics.push(metric);
        } else {
            let result = key_scan(&reader, &field, rows, global.logical_bytes())?;
            totals.decode += result.decode;
            totals.validity += result.validity;
            peak = peak.max(result.logical_bytes);
            all_slots += result.slots;
            all_used_slots += result.used_slots;
            all_present += result.present_cells;
            let dense = rows > 0 && result.present_cells == rows as u64;
            dense_keys += usize::from(dense);
            checksum.bytes(field.as_bytes());
            checksum.u64(0);
            checksum.u64(result.present_cells);
            let metric = FieldMetric {
                name: field,
                mode,
                decode_ns: result.decode.as_nanos(),
                translate_ns: 0,
                construct_ns: result.validity.as_nanos(),
                verify_ns: 0,
                slots: result.slots,
                used_slots: result.used_slots,
                terms: 0,
                postings: 0,
                present_cells: result.present_cells,
                dense,
                digest: 0,
            };
            println!(
                "field scope=candidate field={} mode=key_only verified=false dict_decode_ns={} key_validity_ns={} dictionary_slots={} used_slots={} value_terms=0 postings=0 key_present_cells={} dense_key={} checksum=0000000000000000",
                escape(&metric.name),
                metric.decode_ns,
                metric.construct_ns,
                metric.slots,
                metric.used_slots,
                metric.present_cells,
                metric.dense
            );
            metrics.push(metric);
        }
    }

    metrics.sort_by_key(|metric| Reverse(metric.total_ns()));
    for (rank, metric) in metrics.iter().take(SLOW_CAP).enumerate() {
        println!(
            "slow_field scope=candidate rank={} field={} mode={} total_ns={} dict_decode_ns={} dict_translate_ns={} construct_or_key_ns={} row_verify_ns={} dictionary_slots={} used_slots={} value_terms={} postings={} key_present_cells={}",
            rank + 1,
            escape(&metric.name),
            metric.mode.label(),
            metric.total_ns(),
            metric.decode_ns,
            metric.translate_ns,
            metric.construct_ns,
            metric.verify_ns,
            metric.slots,
            metric.used_slots,
            metric.terms,
            metric.postings,
            metric.present_cells
        );
    }
    let wall = wall_started.elapsed();
    println!("phase_total scope=file phase=open ns={}", open.as_nanos());
    println!(
        "phase_total scope=candidate phase=dictionary_decode ns={}",
        totals.decode.as_nanos()
    );
    println!(
        "phase_total scope=candidate phase=dictionary_translate ns={}",
        totals.translate.as_nanos()
    );
    println!(
        "phase_total scope=candidate phase=dictionary_decode_translate ns={}",
        (totals.decode + totals.translate).as_nanos()
    );
    println!(
        "phase_total scope=candidate phase=csr_count_fill_sort_digest ns={}",
        totals.csr.as_nanos()
    );
    println!(
        "phase_total scope=candidate phase=key_only_validity ns={}",
        totals.validity.as_nanos()
    );
    println!(
        "phase_total scope=reference phase=row_verification ns={}",
        totals.verify.as_nanos()
    );
    println!(
        "phase_total scope=file phase=total_wall ns={}",
        wall.as_nanos()
    );
    println!(
        "summary scope=candidate rows={} schema_fields={} raw_fields={} fts_fields={} bloom_only_fields={} dictionary_slots={} used_slots={} value_terms={} postings={} key_present_cells={} dense_keys={} verified_fields={} peak_logical_candidate_bytes={} checksum={:016x}",
        rows,
        schema.fields().len(),
        raw,
        fts,
        bloom_names.len(),
        all_slots,
        all_used_slots,
        global.count(),
        all_postings,
        all_present,
        dense_keys,
        verify.len(),
        peak,
        checksum.finish()
    );
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let Some(options) = options()? else {
        usage();
        return Ok(());
    };
    run(options)
}
