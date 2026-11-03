//! A strict Rust kernel matching the repository's Python transaction-wire path.
//!
//! The kernel deliberately stops at the same boundary as
//! src/pumpquant/solana_wire.py: it decodes canonical standard base64 and
//! parses the signatures, message header, static account keys, instructions,
//! and v0 address-table lookup envelopes. It returns the first signature and
//! ordered static account keys, but does not resolve loaded addresses or decode
//! instructions, balances, logs, or Borsh payloads.
//!
//! The implementation has no third-party dependencies so it can be embedded by
//! a later Rayon or Python extension without pulling in a larger Solana stack.

use std::fmt;

/// The canonical Bitcoin/Solana base58 alphabet.
pub const BASE58_ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const BASE58_ALPHABET_BYTES: &[u8; 58] =
    b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const SIGNATURE_BYTES: usize = 64;
const PUBKEY_BYTES: usize = 32;
const MAX_ACCOUNT_KEYS: u16 = 256;
const MAX_SIGNATURES: u16 = 255;

/// A successfully parsed transaction's first signature and static account keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedTransaction {
    /// The first transaction signature in canonical Solana base58.
    pub first_signature: String,
    /// The message's ordered static account keys in canonical Solana base58.
    pub static_account_keys: Vec<String>,
}

impl ParsedTransaction {
    /// Return the result in the tuple shape used by the Python reference.
    #[must_use]
    pub fn into_parts(self) -> (String, Vec<String>) {
        (self.first_signature, self.static_account_keys)
    }

    /// Borrow the result in the tuple shape used by callers that avoid moves.
    #[must_use]
    pub fn as_parts(&self) -> (&str, &[String]) {
        (&self.first_signature, &self.static_account_keys)
    }
}

/// Errors raised by strict transaction-wire decoding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SolanaWireError {
    /// The bytes or their enclosing canonical base64 are malformed.
    MalformedTransaction(String),
    /// Only the legacy message and version 0 are supported by this kernel.
    UnsupportedTransactionVersion(u8),
}

impl SolanaWireError {
    fn malformed(message: impl Into<String>) -> Self {
        Self::MalformedTransaction(message.into())
    }

    /// Return whether this error represents malformed bytes/base64.
    #[must_use]
    pub fn is_malformed(&self) -> bool {
        matches!(self, Self::MalformedTransaction(_))
    }

    /// Return whether this error represents an unsupported message version.
    #[must_use]
    pub fn is_unsupported_version(&self) -> bool {
        matches!(self, Self::UnsupportedTransactionVersion(_))
    }
}

impl fmt::Display for SolanaWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedTransaction(message) => formatter.write_str(message),
            Self::UnsupportedTransactionVersion(version) => {
                write!(formatter, "unsupported Solana message version {version}")
            }
        }
    }
}

impl std::error::Error for SolanaWireError {}

/// Canonical Bitcoin/Solana base58 encoding for arbitrary bytes.
///
/// This is public so parity harnesses can build expected signatures and keys
/// without depending on a separate base58 crate. Empty input encodes to the
/// empty string, matching the Python helper's behavior.
#[must_use]
pub fn base58_encode(value: &[u8]) -> String {
    if value.is_empty() {
        return String::new();
    }

    // Digits are kept least-significant first while repeatedly multiplying by
    // 256. A 64-byte signature needs at most 88 base58 digits.
    let mut digits: Vec<u8> = Vec::with_capacity(value.len() * 2);
    digits.push(0);
    for &byte in value {
        let mut carry = u16::from(byte);
        for digit in &mut digits {
            let product = u16::from(*digit) * 256 + carry;
            *digit = (product % 58) as u8;
            carry = product / 58;
        }
        while carry != 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    let leading_zeroes = value.iter().take_while(|&&byte| byte == 0).count();
    if digits.len() == 1 && digits[0] == 0 {
        return "1".repeat(leading_zeroes);
    }
    let mut encoded = String::with_capacity(leading_zeroes + digits.len());
    for _ in 0..leading_zeroes {
        encoded.push('1');
    }
    for &digit in digits.iter().rev() {
        encoded.push(BASE58_ALPHABET_BYTES[usize::from(digit)] as char);
    }
    encoded
}

/// Decode canonical standard RFC 4648 base64 transaction text.
///
/// The decoder intentionally does not accept URL-safe characters, whitespace,
/// omitted padding, or non-zero unused pad bits. Re-encoding the decoded bytes
/// provides the same canonicality check as the Python reference.
fn decode_base64(value: &str) -> Result<Vec<u8>, SolanaWireError> {
    if value.is_empty() {
        return Err(SolanaWireError::malformed(
            "serialized transaction base64 is empty",
        ));
    }
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(SolanaWireError::malformed(
            "serialized transaction base64 is invalid",
        ));
    }

    let mut decoded = Vec::with_capacity((bytes.len() / 4) * 3);
    for (group_index, quartet) in bytes.chunks_exact(4).enumerate() {
        let last_group = group_index + 1 == bytes.len() / 4;
        let first = decode_base64_digit(quartet[0]);
        let second = decode_base64_digit(quartet[1]);
        let (first, second) = match (first, second) {
            (Some(first), Some(second)) => (first, second),
            _ => {
                return Err(SolanaWireError::malformed(
                    "serialized transaction base64 is invalid",
                ));
            }
        };
        decoded.push((first << 2) | (second >> 4));

        match (quartet[2], quartet[3]) {
            (b'=', b'=') => {
                if !last_group {
                    return Err(SolanaWireError::malformed(
                        "serialized transaction base64 is invalid",
                    ));
                }
                // The canonical re-encode below also rejects non-zero pad bits.
            }
            (third, b'=') => {
                let third = decode_base64_digit(third).ok_or_else(|| {
                    SolanaWireError::malformed("serialized transaction base64 is invalid")
                })?;
                if !last_group {
                    return Err(SolanaWireError::malformed(
                        "serialized transaction base64 is invalid",
                    ));
                }
                decoded.push((second << 4) | (third >> 2));
            }
            (third, fourth) => {
                let third = decode_base64_digit(third).ok_or_else(|| {
                    SolanaWireError::malformed("serialized transaction base64 is invalid")
                })?;
                let fourth = decode_base64_digit(fourth).ok_or_else(|| {
                    SolanaWireError::malformed("serialized transaction base64 is invalid")
                })?;
                decoded.push((second << 4) | (third >> 2));
                decoded.push((third << 6) | fourth);
            }
        }
    }

    if encode_base64(&decoded) != value {
        return Err(SolanaWireError::malformed(
            "serialized transaction base64 is not canonical",
        ));
    }
    Ok(decoded)
}

fn decode_base64_digit(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

/// Decode one canonical standard-base64 value using the kernel's strict rules.
///
/// Transaction callers normally use [`parse_serialized_transaction`], but this
/// entry point keeps the base64 primitive available for a future batch/FFI
/// layer without exposing the cursor internals.
pub fn decode_transaction_base64(value: &str) -> Result<Vec<u8>, SolanaWireError> {
    decode_base64(value)
}

fn encode_base64(value: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let groups = value.len().div_ceil(3);
    let mut encoded = String::with_capacity(groups * 4);
    for chunk in value.chunks(3) {
        let first = chunk[0];
        encoded.push(ALPHABET[usize::from(first >> 2)] as char);
        if chunk.len() == 1 {
            encoded.push(ALPHABET[usize::from((first & 0x03) << 4)] as char);
            encoded.push('=');
            encoded.push('=');
            continue;
        }
        let second = chunk[1];
        encoded.push(ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))] as char);
        if chunk.len() == 2 {
            encoded.push(ALPHABET[usize::from((second & 0x0f) << 2)] as char);
            encoded.push('=');
            continue;
        }
        let third = chunk[2];
        encoded.push(ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))] as char);
        encoded.push(ALPHABET[usize::from(third & 0x3f)] as char);
    }
    encoded
}

struct Cursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.payload.len() - self.offset
    }

    fn read(&mut self, count: usize, label: &str) -> Result<&'a [u8], SolanaWireError> {
        let end = self.offset.checked_add(count).ok_or_else(|| {
            SolanaWireError::malformed(format!("serialized transaction is truncated at {label}"))
        })?;
        if end > self.payload.len() {
            return Err(SolanaWireError::malformed(format!(
                "serialized transaction is truncated at {label}"
            )));
        }
        let value = &self.payload[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self, label: &str) -> Result<u8, SolanaWireError> {
        Ok(self.read(1, label)?[0])
    }

    fn read_shortvec(&mut self, label: &str) -> Result<u16, SolanaWireError> {
        let mut value: u16 = 0;
        let mut shift = 0;
        for group in 0..3 {
            let byte = self.read_u8(&format!("{label} shortvec"))?;
            let part = byte & 0x7f;
            if group == 2 && (byte & 0x80 != 0 || part > 0x03) {
                return Err(SolanaWireError::malformed(format!(
                    "{label} shortvec exceeds u16"
                )));
            }
            value |= u16::from(part) << shift;
            if byte & 0x80 == 0 {
                if group > 0 && value < (1u16 << (7 * group)) {
                    return Err(SolanaWireError::malformed(format!(
                        "{label} shortvec is noncanonical"
                    )));
                }
                return Ok(value);
            }
            shift += 7;
        }
        Err(SolanaWireError::malformed(format!(
            "{label} shortvec is unterminated"
        )))
    }
}

fn read_account_indexes(
    cursor: &mut Cursor<'_>,
    count: u16,
    label: &str,
    static_count: u16,
) -> Result<Vec<u8>, SolanaWireError> {
    if count > MAX_ACCOUNT_KEYS {
        return Err(SolanaWireError::malformed(format!(
            "{label} count exceeds u8 account-index bounds"
        )));
    }
    let mut indexes = Vec::with_capacity(usize::from(count));
    for _ in 0..count {
        indexes.push(cursor.read_u8(&format!("{label} index"))?);
    }
    if static_count == 0 {
        return Err(SolanaWireError::malformed(
            "message has no static account keys",
        ));
    }
    Ok(indexes)
}

/// Parse one canonical base64-encoded Solana legacy or v0 transaction.
///
/// The result contains exactly the first signature and ordered static account
/// keys. Unsupported versions, noncanonical shortvec/base64 values, truncated
/// fields, out-of-range account indexes under the Python contract, and trailing
/// bytes return an error. Loaded address-table keys are counted but not
/// resolved or runtime-sanitized, matching the Python reference parser.
pub fn parse_serialized_transaction(value: &str) -> Result<ParsedTransaction, SolanaWireError> {
    let payload = decode_base64(value)?;
    let mut cursor = Cursor::new(&payload);

    let signature_count = cursor.read_shortvec("signature count")?;
    if !(1..=MAX_SIGNATURES).contains(&signature_count) {
        return Err(SolanaWireError::malformed(
            "signature count is outside Solana transaction bounds",
        ));
    }
    let signatures = cursor.read(usize::from(signature_count) * SIGNATURE_BYTES, "signatures")?;

    let first_message_byte = cursor.read_u8("message version/header")?;
    let (versioned, num_required_signatures) = if first_message_byte & 0x80 != 0 {
        let version = first_message_byte & 0x7f;
        if version != 0 {
            return Err(SolanaWireError::UnsupportedTransactionVersion(version));
        }
        (true, cursor.read_u8("required signature count")?)
    } else {
        (false, first_message_byte)
    };
    let num_readonly_signed = cursor.read_u8("readonly signed account count")?;
    let num_readonly_unsigned = cursor.read_u8("readonly unsigned account count")?;

    if usize::from(num_required_signatures) != usize::from(signature_count) {
        return Err(SolanaWireError::malformed(
            "signature count does not match message required signature count",
        ));
    }
    if num_required_signatures == 0 {
        return Err(SolanaWireError::malformed(
            "message requires at least one signature",
        ));
    }
    if num_readonly_signed > num_required_signatures {
        return Err(SolanaWireError::malformed(
            "readonly signed account count exceeds required signatures",
        ));
    }

    let static_count = cursor.read_shortvec("static account-key count")?;
    if static_count < u16::from(num_required_signatures) || static_count > MAX_ACCOUNT_KEYS {
        return Err(SolanaWireError::malformed(
            "static account-key count is outside message bounds",
        ));
    }
    if u16::from(num_readonly_unsigned) > static_count - u16::from(num_required_signatures) {
        return Err(SolanaWireError::malformed(
            "readonly unsigned account count exceeds unsigned static accounts",
        ));
    }

    let mut static_account_keys = Vec::with_capacity(usize::from(static_count));
    for index in 0..static_count {
        let key = cursor.read(PUBKEY_BYTES, &format!("static account key {index}"))?;
        static_account_keys.push(base58_encode(key));
    }
    cursor.read(PUBKEY_BYTES, "recent blockhash")?;

    let instruction_count = cursor.read_shortvec("instruction count")?;
    let mut instruction_indexes = Vec::new();
    for instruction_index in 0..instruction_count {
        instruction_indexes
            .push(cursor.read_u8(&format!("instruction {instruction_index} program index"))?);
        let account_count =
            cursor.read_shortvec(&format!("instruction {instruction_index} account count"))?;
        instruction_indexes.extend(read_account_indexes(
            &mut cursor,
            account_count,
            &format!("instruction {instruction_index} account"),
            static_count,
        )?);
        let data_count =
            cursor.read_shortvec(&format!("instruction {instruction_index} data length"))?;
        cursor.read(
            usize::from(data_count),
            &format!("instruction {instruction_index} data"),
        )?;
    }

    let mut loaded_count: usize = 0;
    if versioned {
        let lookup_count = cursor.read_shortvec("address-table lookup count")?;
        for lookup_index in 0..lookup_count {
            cursor.read(
                PUBKEY_BYTES,
                &format!("address-table lookup {lookup_index} account"),
            )?;
            let writable_count = cursor.read_shortvec(&format!(
                "address-table lookup {lookup_index} writable index count"
            ))?;
            cursor.read(
                usize::from(writable_count),
                &format!("address-table lookup {lookup_index} writable indexes"),
            )?;
            let readonly_count = cursor.read_shortvec(&format!(
                "address-table lookup {lookup_index} readonly index count"
            ))?;
            cursor.read(
                usize::from(readonly_count),
                &format!("address-table lookup {lookup_index} readonly indexes"),
            )?;
            loaded_count = loaded_count
                .checked_add(usize::from(writable_count))
                .and_then(|count| count.checked_add(usize::from(readonly_count)))
                .ok_or_else(|| {
                    SolanaWireError::malformed("total account keys exceed u8 index bounds")
                })?;
            if usize::from(static_count) + loaded_count > usize::from(MAX_ACCOUNT_KEYS) {
                return Err(SolanaWireError::malformed(
                    "total account keys exceed u8 index bounds",
                ));
            }
        }
    }

    let total_accounts = usize::from(static_count) + loaded_count;
    if instruction_indexes
        .iter()
        .any(|&index| usize::from(index) >= total_accounts)
    {
        return Err(SolanaWireError::malformed(
            "instruction account index is outside account-key bounds",
        ));
    }
    if cursor.remaining() != 0 {
        return Err(SolanaWireError::malformed(
            "serialized transaction has trailing bytes",
        ));
    }

    let first_signature = base58_encode(&signatures[..SIGNATURE_BYTES]);
    Ok(ParsedTransaction {
        first_signature,
        static_account_keys,
    })
}

/// Parse the same wire value and return the Python-compatible tuple shape.
pub fn parse_serialized_transaction_parts(
    value: &str,
) -> Result<(String, Vec<String>), SolanaWireError> {
    Ok(parse_serialized_transaction(value)?.into_parts())
}

/// Parse a batch independently, preserving input order and per-record errors.
///
/// This API intentionally returns a result per input instead of failing fast;
/// a future Rayon/Python adapter can parallelize the independent calls while
/// retaining deterministic order and the same error classification.
pub fn parse_serialized_transactions(
    values: &[&str],
) -> Vec<Result<ParsedTransaction, SolanaWireError>> {
    values
        .iter()
        .map(|value| parse_serialized_transaction(value))
        .collect()
}

/// Parse a batch held in owned strings without copying the input values.
pub fn parse_serialized_transaction_strings(
    values: &[String],
) -> Vec<Result<ParsedTransaction, SolanaWireError>> {
    values
        .iter()
        .map(|value| parse_serialized_transaction(value))
        .collect()
}

/// Alias emphasizing that this is a batch kernel entry point.
pub fn parse_batch(values: &[&str]) -> Vec<Result<ParsedTransaction, SolanaWireError>> {
    parse_serialized_transactions(values)
}

/// Names matching common callers and the Python reference aliases.
pub use parse_serialized_transaction as parse_transaction;
/// Names matching common callers and the Python reference aliases.
pub use parse_serialized_transaction as parse_transaction_base64;
/// Names matching common callers and the Python reference aliases.
pub use parse_serialized_transaction as parse_solana_transaction;
/// Names matching common callers and the Python reference aliases.
pub use parse_serialized_transaction as parse_transaction_wire;
/// Names matching common callers and the Python reference aliases.
pub use parse_serialized_transaction as decode_solana_transaction;

#[cfg(test)]
mod tests {
    use super::*;

    fn shortvec(mut value: u16) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                result.push(byte | 0x80);
            } else {
                result.push(byte);
                return result;
            }
        }
    }

    fn key(index: u8) -> [u8; PUBKEY_BYTES] {
        [index; PUBKEY_BYTES]
    }

    fn serialized(
        version: Option<u8>,
        static_keys: &[[u8; PUBKEY_BYTES]],
        instructions: &[(u8, &[u8], &[u8])],
        lookups: &[([u8; PUBKEY_BYTES], &[u8], &[u8])],
        signature: [u8; SIGNATURE_BYTES],
    ) -> Vec<u8> {
        let mut message = Vec::new();
        if let Some(version) = version {
            message.push(0x80 | version);
            message.extend_from_slice(&[1, 0, 0]);
        } else {
            message.extend_from_slice(&[1, 0, 0]);
        }
        message.extend(shortvec(static_keys.len() as u16));
        for static_key in static_keys {
            message.extend_from_slice(static_key);
        }
        message.extend_from_slice(&key(7));
        message.extend(shortvec(instructions.len() as u16));
        for &(program_index, account_indexes, data) in instructions {
            message.push(program_index);
            message.extend(shortvec(account_indexes.len() as u16));
            message.extend_from_slice(account_indexes);
            message.extend(shortvec(data.len() as u16));
            message.extend_from_slice(data);
        }
        if version.is_some() {
            message.extend(shortvec(lookups.len() as u16));
            for (lookup_key, writable, readonly) in lookups {
                message.extend_from_slice(lookup_key);
                message.extend(shortvec(writable.len() as u16));
                message.extend_from_slice(writable);
                message.extend(shortvec(readonly.len() as u16));
                message.extend_from_slice(readonly);
            }
        }
        let mut payload = Vec::with_capacity(1 + SIGNATURE_BYTES + message.len());
        payload.push(1);
        payload.extend_from_slice(&signature);
        payload.extend_from_slice(&message);
        payload
    }

    fn encoded(value: &[u8]) -> String {
        encode_base64(value)
    }

    #[test]
    fn base58_matches_known_values() {
        assert_eq!(base58_encode(&[]), "");
        assert_eq!(base58_encode(&[0]), "1");
        assert_eq!(base58_encode(&[0, 0, 1]), "112");
        assert_eq!(base58_encode(&[1]), "2");
        assert_eq!(base58_encode(&[9; 4]), "EPvZN");
    }

    #[test]
    fn legacy_returns_signature_and_ordered_static_keys() {
        let signature = [9; SIGNATURE_BYTES];
        let static_keys = [key(1), key(2)];
        let result = parse_serialized_transaction(&encoded(&serialized(
            None,
            &static_keys,
            &[],
            &[],
            signature,
        )))
        .expect("legacy fixture should parse");
        assert_eq!(result.first_signature, base58_encode(&signature));
        assert_eq!(
            result.static_account_keys,
            vec![base58_encode(&key(1)), base58_encode(&key(2))]
        );
    }

    #[test]
    fn v0_consumes_lookup_envelope_and_allows_loaded_account_indexes() {
        let signature = [9; SIGNATURE_BYTES];
        let static_keys = [key(1), key(2)];
        let lookups = [(key(3), &[4u8][..], &[8u8][..])];
        let instructions = [(2u8, &[2u8][..], &[1u8][..])];
        let result = parse_serialized_transaction(&encoded(&serialized(
            Some(0),
            &static_keys,
            &instructions,
            &lookups,
            signature,
        )))
        .expect("v0 fixture should parse");
        assert_eq!(
            result.static_account_keys,
            vec![base58_encode(&key(1)), base58_encode(&key(2))]
        );
    }

    #[test]
    fn batch_preserves_order_and_errors() {
        let signature = [9; SIGNATURE_BYTES];
        let static_keys = [key(1), key(2)];
        let valid = encoded(&serialized(None, &static_keys, &[], &[], signature));
        let mut invalid_payload = serialized(None, &static_keys, &[], &[], signature);
        invalid_payload.push(0);
        let invalid = encoded(&invalid_payload);
        let values = [&valid[..], &invalid[..]];
        let results = parse_batch(&values);
        assert!(results[0].is_ok());
        assert_eq!(
            results[0]
                .as_ref()
                .expect("valid result")
                .static_account_keys,
            vec![base58_encode(&key(1)), base58_encode(&key(2))]
        );
        assert!(matches!(
            results[1].as_ref(),
            Err(SolanaWireError::MalformedTransaction(_))
        ));
    }

    #[test]
    fn malformed_shortvec_trailing_and_header_mismatch_fail() {
        let signature = [9; SIGNATURE_BYTES];
        let static_keys = [key(1), key(2)];
        let payload = serialized(None, &static_keys, &[], &[], signature);

        let mut noncanonical = payload.clone();
        noncanonical.splice(0..1, [0x81, 0x00]);
        assert!(matches!(
            parse_serialized_transaction(&encoded(&noncanonical)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));

        let mut trailing = payload.clone();
        trailing.push(0);
        assert!(matches!(
            parse_serialized_transaction(&encoded(&trailing)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));

        let mut mismatched_header = payload;
        mismatched_header[65] = 2;
        assert!(matches!(
            parse_serialized_transaction(&encoded(&mismatched_header)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));
    }

    #[test]
    fn unsupported_version_is_classified_separately() {
        let payload = serialized(Some(1), &[key(1), key(2)], &[], &[], [9; SIGNATURE_BYTES]);
        assert_eq!(
            parse_serialized_transaction(&encoded(&payload)),
            Err(SolanaWireError::UnsupportedTransactionVersion(1))
        );
    }

    #[test]
    fn base64_is_strict_and_canonical() {
        assert!(decode_base64("AA==").is_ok());
        for value in ["", "A", "AA=", "AA===", "A A=", "_w==", "AB=="] {
            assert!(decode_base64(value).is_err(), "accepted {value:?}");
        }
        assert_eq!(decode_base64("TQ==").expect("canonical"), b"M");
        assert_eq!(decode_base64("TWE=").expect("canonical"), b"Ma");
        assert_eq!(decode_base64("TWFu").expect("canonical"), b"Man");
    }

    #[test]
    fn account_and_signature_bounds_are_checked() {
        let signature = [9; SIGNATURE_BYTES];
        let mut payload = serialized(None, &[key(1)], &[], &[], signature);
        // The fixture has one signature, then a legacy header at offset 65.
        payload[65] = 0;
        assert!(matches!(
            parse_serialized_transaction(&encoded(&payload)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));

        let mut too_many_signatures = payload.clone();
        too_many_signatures[0] = 0;
        assert!(matches!(
            parse_serialized_transaction(&encoded(&too_many_signatures)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));
    }

    #[test]
    fn instruction_account_bounds_are_checked_for_legacy_and_v0() {
        let signature = [9; SIGNATURE_BYTES];
        let static_keys = [key(1), key(2)];
        let legacy_instructions = [(2u8, &[2u8][..], &[][..])];
        let legacy = serialized(None, &static_keys, &legacy_instructions, &[], signature);
        assert!(matches!(
            parse_serialized_transaction(&encoded(&legacy)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));

        let v0_instructions = [(3u8, &[3u8][..], &[][..])];
        let v0 = serialized(Some(0), &static_keys, &v0_instructions, &[], signature);
        assert!(matches!(
            parse_serialized_transaction(&encoded(&v0)),
            Err(SolanaWireError::MalformedTransaction(_))
        ));
    }
}
