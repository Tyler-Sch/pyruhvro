#![allow(dead_code)]
use anyhow::Result;

// Useful for testing
pub fn decode_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("Error unwrapping hex"))
        .collect::<Vec<_>>()
}
