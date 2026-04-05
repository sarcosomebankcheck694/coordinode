//! CJK tokenization: lindera (JP/KR) + jieba-rs (CN).
//!
//! Provides tantivy-compatible tokenizers for Chinese, Japanese, and Korean text.
//! Each tokenizer wraps a dictionary-based segmenter that splits text into
//! meaningful words rather than whitespace-delimited tokens.
//!
//! Feature flags (per-language, include only what you need):
//! - `cjk-zh` — `"chinese_jieba"` analyzer (jieba-rs, +21MB)
//! - `cjk-ja` — `"japanese_lindera"` analyzer (lindera IPAdic, +15MB)
//! - `cjk-ko` — `"korean_lindera"` analyzer (lindera KO-dic, +34MB)
//! - `cjk` — umbrella: all three (~70MB)

#[cfg(any(feature = "cjk-zh", feature = "cjk-ja", feature = "cjk-ko"))]
mod tokenizers;

#[cfg(feature = "cjk-zh")]
pub use tokenizers::JiebaTokenizer;

#[cfg(any(feature = "cjk-ja", feature = "cjk-ko"))]
pub use tokenizers::{CjkLanguage, LinderaTokenizer};
