// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

/// Maximum encoded length of an exact segment-id provenance token.
///
/// The token is one filesystem path component. Reserving `l0_h2_`, the
/// delimiter, a signed 64-bit hour index, and `.parquet` leaves 220 bytes
/// within the portable 255-byte component limit. The decoded payload is at
/// most 165 bytes.
pub const MAX_EXACT_TOKEN_LEN: usize = 220;

/// Segment provenance carried by an L0 object key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum L0Provenance {
    /// Legacy and h1 planners encode every id in an inclusive range.
    Range(i64, i64),
    /// The h2 planner encodes only the source ids actually present.
    Exact(Vec<i64>),
}

/// Encode sorted, positive source segment ids as unsigned delta varints and
/// URL-safe base64 without padding.
pub fn encode_exact_ids(ids: &[i64]) -> anyhow::Result<String> {
    anyhow::ensure!(!ids.is_empty(), "L0 provenance ids must not be empty");

    // The final token cap bounds useful encoded bytes. Stop growing as soon as
    // exceeding it is certain rather than allocating in proportion to input.
    let max_decoded_len = MAX_EXACT_TOKEN_LEN / 4 * 3;
    let mut encoded = Vec::with_capacity(ids.len().min(max_decoded_len));
    let mut previous = 0_i64;
    for (index, &id) in ids.iter().enumerate() {
        anyhow::ensure!(id > 0, "L0 provenance ids must be positive");
        anyhow::ensure!(
            index == 0 || id > previous,
            "L0 provenance ids must be strictly increasing"
        );
        let value = if index == 0 {
            id as u64
        } else {
            (id - previous) as u64
        };
        push_varint(value, &mut encoded);
        anyhow::ensure!(
            encoded.len() <= max_decoded_len,
            "L0 provenance token exceeds {MAX_EXACT_TOKEN_LEN} bytes"
        );
        previous = id;
    }

    let token = URL_SAFE_NO_PAD.encode(encoded);
    anyhow::ensure!(
        token.len() <= MAX_EXACT_TOKEN_LEN,
        "L0 provenance token exceeds {MAX_EXACT_TOKEN_LEN} bytes"
    );
    Ok(token)
}

/// Parse provenance from an L0 object key.
///
/// Malformed h2 keys return `None` and are never reinterpreted as a legacy
/// range: malformed provenance must not suppress queryable segments.
pub fn parse_l0_provenance(key: &str) -> Option<L0Provenance> {
    let name = key.rsplit('/').next()?;
    if name.starts_with("l0_h2_") {
        return parse_h2(name).map(L0Provenance::Exact);
    }
    parse_legacy_range(name).map(|(min, max)| L0Provenance::Range(min, max))
}

fn push_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn parse_h2(name: &str) -> Option<Vec<i64>> {
    let (stem, extension) = name.rsplit_once('.')?;
    if extension.is_empty() {
        return None;
    }
    let rest = stem.strip_prefix("l0_h2_")?;
    // URL-safe base64 itself may contain `_`, so only the final underscore is
    // the delimiter before the numeric hour index.
    let (token, hour_index) = rest.rsplit_once('_')?;
    if token.is_empty() || token.len() > MAX_EXACT_TOKEN_LEN || hour_index.is_empty() {
        return None;
    }

    // Numeric spelling is part of the deterministic key format.
    let hour: u64 = hour_index.parse().ok()?;
    if hour.to_string() != hour_index {
        return None;
    }

    let bytes = URL_SAFE_NO_PAD.decode(token).ok()?;
    if bytes.is_empty() || URL_SAFE_NO_PAD.encode(&bytes) != token {
        return None;
    }

    let mut ids = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    let first = read_varint(&bytes, &mut cursor)?;
    let mut current = i64::try_from(first).ok()?;
    if current <= 0 {
        return None;
    }
    ids.push(current);

    while cursor < bytes.len() {
        let delta = read_varint(&bytes, &mut cursor)?;
        if delta == 0 {
            return None;
        }
        let delta = i64::try_from(delta).ok()?;
        current = current.checked_add(delta)?;
        ids.push(current);
    }
    Some(ids)
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value = 0_u64;
    for byte_index in 0..10 {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        if byte_index == 9 && byte > 1 {
            return None;
        }
        value |= u64::from(byte & 0x7f) << (byte_index * 7);
        if byte & 0x80 == 0 {
            // A zero terminal group after another byte is a redundant,
            // noncanonical varint encoding.
            return (byte_index == 0 || byte != 0).then_some(value);
        }
    }
    None
}

fn parse_legacy_range(name: &str) -> Option<(i64, i64)> {
    let stem = name.rsplit_once('.').map(|(stem, _)| stem).unwrap_or(name);
    let rest = stem.strip_prefix("l0_")?;
    let mut parts = rest.rsplit('_');
    let _hour_or_part: u64 = parts.next()?.parse().ok()?;
    let max: i64 = parts.next()?.parse().ok()?;
    let min: i64 = parts.next()?.parse().ok()?;
    // At least the writer/planner field must remain. Its historic spelling is
    // deliberately not restricted, preserving legacy and h1 parsing.
    parts.next()?;
    (min >= 1 && max >= min).then_some((min, max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_ids_round_trip_in_a_canonical_key() {
        let ids = [255, 256, 383, 16_384, i64::MAX];
        let token = encode_exact_ids(&ids).unwrap();
        assert!(
            token.contains('_'),
            "test token must exercise the delimiter character"
        );
        assert!(token.chars().all(|ch| ch != '=' && ch != '+' && ch != '/'));
        assert_eq!(
            parse_l0_provenance(&format!(
                "files/o/logs/s/2026/09/02/10/l0_h2_{token}_496682.vix"
            )),
            Some(L0Provenance::Exact(ids.to_vec()))
        );
    }

    #[test]
    fn encoder_rejects_invalid_and_oversized_inputs() {
        for ids in [&[][..], &[0][..], &[-1][..], &[1, 1][..], &[2, 1][..]] {
            assert!(encode_exact_ids(ids).is_err(), "accepted {ids:?}");
        }
        let ids: Vec<i64> = (1..=400).collect();
        assert!(encode_exact_ids(&ids).is_err());
    }

    #[test]
    fn h2_parser_rejects_malformed_oversized_and_noncanonical_keys() {
        let noncanonical_varint = URL_SAFE_NO_PAD.encode([0x81, 0x00]);
        let zero_delta = URL_SAFE_NO_PAD.encode([1, 0]);
        let overflowing_varint = URL_SAFE_NO_PAD.encode([0xff; 10]);
        let oversized = "A".repeat(MAX_EXACT_TOKEN_LEN + 1);
        for key in [
            "l0_h2__1.vix".to_string(),
            "l0_h2_AA_1.vix".to_string(), // first id is zero
            "l0_h2_AQ==_1.vix".to_string(),
            "l0_h2_AQ_01.vix".to_string(),
            "l0_h2_AQ_1".to_string(),
            "l0_h2_AQ_1.".to_string(),
            "l0_h2_AQ_1_extra.vix".to_string(),
            format!("l0_h2_{noncanonical_varint}_1.vix"),
            format!("l0_h2_{zero_delta}_1.vix"),
            format!("l0_h2_{overflowing_varint}_1.vix"),
            format!("l0_h2_{oversized}_1.vix"),
        ] {
            assert_eq!(parse_l0_provenance(&key), None, "accepted {key:?}");
        }
    }

    #[test]
    fn legacy_and_h1_ranges_keep_historic_parsing() {
        for (key, expected) in [
            ("l0_multi_5_7_2", Some(L0Provenance::Range(5, 7))),
            (
                "files/o/logs/s/l0_h1_node_a_10_10_496682.vix",
                Some(L0Provenance::Range(10, 10)),
            ),
            ("l0__2_9_3", Some(L0Provenance::Range(2, 9))),
            ("l0_x_2_9_3.", Some(L0Provenance::Range(2, 9))),
            ("l0_x_9_2_3.vix", None),
        ] {
            assert_eq!(parse_l0_provenance(key), expected, "key {key:?}");
        }
    }

    #[test]
    fn malformed_h2_never_falls_back_to_a_numeric_range() {
        assert_eq!(parse_l0_provenance("l0_h2_2_9_3.vix"), None);
    }
}
