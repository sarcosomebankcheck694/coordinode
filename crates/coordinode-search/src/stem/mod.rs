//! 30+ language stemmers via Snowball (rust-stemmers).
//!
//! Provides per-language stemmer selection and a convenience function
//! to stem text using a named language. The `"auto"` language uses
//! whatlang-rs to detect the language automatically.

use rust_stemmers::{Algorithm, Stemmer};
use std::borrow::Cow;

/// Supported stemmer language names.
///
/// Maps string names (as used in analyzer config) to rust-stemmers Algorithm.
/// Returns `None` for unknown languages or CJK (which need tokenizers, not stemmers).
pub fn algorithm_for_language(lang: &str) -> Option<Algorithm> {
    match lang.to_lowercase().as_str() {
        "arabic" | "ar" => Some(Algorithm::Arabic),
        "armenian" | "hy" => Some(Algorithm::Armenian),
        "danish" | "da" => Some(Algorithm::Danish),
        "dutch" | "nl" => Some(Algorithm::Dutch),
        "english" | "en" => Some(Algorithm::English),
        "finnish" | "fi" => Some(Algorithm::Finnish),
        "french" | "fr" => Some(Algorithm::French),
        "german" | "de" => Some(Algorithm::German),
        "greek" | "el" => Some(Algorithm::Greek),
        "hungarian" | "hu" => Some(Algorithm::Hungarian),
        "italian" | "it" => Some(Algorithm::Italian),
        "norwegian" | "no" | "nb" => Some(Algorithm::Norwegian),
        "portuguese" | "pt" => Some(Algorithm::Portuguese),
        "romanian" | "ro" => Some(Algorithm::Romanian),
        "russian" | "ru" => Some(Algorithm::Russian),
        "spanish" | "es" => Some(Algorithm::Spanish),
        "swedish" | "sv" => Some(Algorithm::Swedish),
        "tamil" | "ta" => Some(Algorithm::Tamil),
        "turkish" | "tr" => Some(Algorithm::Turkish),
        "ukrainian" | "uk" => Some(Algorithm::Ukrainian),
        _ => None,
    }
}

/// Stem a single word using the specified language.
///
/// Returns the stemmed form, or the original word if the language
/// is not supported.
pub fn stem_word<'a>(word: &'a str, lang: &str) -> Cow<'a, str> {
    match algorithm_for_language(lang) {
        Some(algo) => Stemmer::create(algo).stem(word),
        None => Cow::Borrowed(word),
    }
}

/// List all supported stemmer language names.
pub fn supported_languages() -> &'static [&'static str] {
    &[
        "arabic",
        "armenian",
        "danish",
        "dutch",
        "english",
        "finnish",
        "french",
        "german",
        "greek",
        "hungarian",
        "italian",
        "norwegian",
        "portuguese",
        "romanian",
        "russian",
        "spanish",
        "swedish",
        "tamil",
        "turkish",
        "ukrainian",
    ]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn english_stemming() {
        assert_eq!(stem_word("running", "english"), "run");
        assert_eq!(stem_word("fruitlessly", "en"), "fruitless");
    }

    #[test]
    fn russian_stemming() {
        assert_eq!(stem_word("бегущий", "russian"), "бегущ");
    }

    #[test]
    fn ukrainian_stemming() {
        assert_eq!(stem_word("книга", "ukrainian"), "книг");
        assert_eq!(stem_word("учитель", "uk"), "учител");
        assert_eq!(stem_word("братові", "ukrainian"), "брат");
    }

    #[test]
    fn german_stemming() {
        assert_eq!(stem_word("aufeinanderschlügen", "de"), "aufeinanderschlug");
    }

    #[test]
    fn unknown_language_returns_original() {
        assert_eq!(stem_word("hello", "klingon"), "hello");
    }

    #[test]
    fn supported_languages_count() {
        assert_eq!(supported_languages().len(), 20);
    }

    #[test]
    fn algorithm_for_all_supported() {
        for lang in supported_languages() {
            assert!(
                algorithm_for_language(lang).is_some(),
                "algorithm_for_language({lang}) should return Some"
            );
        }
    }

    #[test]
    fn iso_code_aliases() {
        assert!(algorithm_for_language("ar").is_some());
        assert!(algorithm_for_language("fr").is_some());
        assert!(algorithm_for_language("uk").is_some());
        assert!(algorithm_for_language("ro").is_some());
    }
}
