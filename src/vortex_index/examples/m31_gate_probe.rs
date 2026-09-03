// M31 probe: why does the #46 column-derived rebuild gate never engage in
// prod? Opens real .vix files (paths as args), prints per-file has_index +
// docs schema, then reports exactly the conditions build_merge_plan checks:
// cross-input type flips and non-value-indexed column types.
//
// Usage: cargo run -p vortex_index --example m31_gate_probe -- FILE.vix ...

use std::collections::BTreeMap;

use arrow_schema::DataType;
use vortex_index::{BytesRangeSource, VixReader, is_value_indexed_type};

fn main() -> anyhow::Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    // optional: --registry schema.json (the stream's latest arrow schema,
    // exactly what build_merge_plan's `latest_schema` types targets from)
    let registry: Option<arrow_schema::Schema> = match args.first().map(String::as_str) {
        Some("--registry") => {
            args.remove(0);
            let path = args.remove(0);
            {
                // the meta table stores a JSON ARRAY of schema versions
                let versions: Vec<arrow_schema::Schema> =
                    serde_json::from_str(&std::fs::read_to_string(path)?)?;
                versions.into_iter().last()
            }
        }
        _ => None,
    };
    let paths = args;
    assert!(!paths.is_empty(), "pass .vix paths");
    // (name -> first-seen type, first-seen file)
    let mut available: BTreeMap<String, (DataType, String)> = BTreeMap::new();
    let mut flips = 0usize;
    let mut underivable = 0usize;
    for path in &paths {
        let bytes = std::fs::read(path)?;
        let source = BytesRangeSource::new(path.clone(), bytes.into());
        let sidecar = std::path::Path::new(path).with_extension("vxi");
        let index = std::fs::read(&sidecar)
            .ok()
            .map(|ib| BytesRangeSource::new(sidecar.display().to_string(), ib.into()));
        let reader = VixReader::open_ranged_with_index(source, index)?;
        let schema = reader.docs_schema()?;
        println!(
            "== {path}\n   has_index={} rows={} fields={}",
            reader.has_index(),
            reader.row_count(),
            schema.fields().len()
        );
        for field in schema.fields() {
            let name = field.name().as_str();
            if matches!(name, "_timestamp" | "_source" | "_original") {
                continue;
            }
            let derivable = is_value_indexed_type(field.data_type()) || name == "_o2_id";
            if !derivable {
                underivable += 1;
                println!("   UNDERIVABLE {name}: {:?}", field.data_type());
            }
            match available.get(name) {
                None => {
                    available.insert(name.to_string(), (field.data_type().clone(), path.clone()));
                }
                Some((seen, first_file)) if seen != field.data_type() => {
                    flips += 1;
                    println!(
                        "   TYPE FLIP {name}: {:?} here vs {:?} first seen in {first_file}",
                        field.data_type(),
                        seen
                    );
                }
                Some(_) => {}
            }
        }
    }
    let mut target_mismatches = 0usize;
    let mut registry_missing = 0usize;
    if let Some(registry) = &registry {
        for (name, (stored, _)) in &available {
            match registry.field_with_name(name) {
                Err(_) => {
                    registry_missing += 1;
                    // target falls back to stored type -> gate passes; count only
                }
                Ok(field) if field.data_type() != stored => {
                    target_mismatches += 1;
                    if target_mismatches <= 20 {
                        println!(
                            "   TARGET MISMATCH {name}: stored {stored:?} vs registry {:?}",
                            field.data_type()
                        );
                    }
                }
                Ok(_) => {}
            }
        }
    }
    println!(
        "\nsummary: files={} union_fields={} type_flips={flips} underivable={underivable} \
         target_mismatches={target_mismatches} registry_missing={registry_missing}",
        paths.len(),
        available.len()
    );
    Ok(())
}
