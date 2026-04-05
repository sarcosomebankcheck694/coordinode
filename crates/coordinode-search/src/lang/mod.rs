//! Language detection via whatlang-rs.
//!
//! Detects the language of a text string using trigram analysis.
//! Returns a language name compatible with `stem::algorithm_for_language()`.

/// Detected language result.
#[derive(Debug, Clone, PartialEq)]
pub struct DetectedLanguage {
    /// Language name (e.g., "english", "russian", "ukrainian").
    pub name: &'static str,
    /// ISO 639-1 code (e.g., "en", "ru", "uk").
    pub code: &'static str,
    /// Detection confidence (0.0..1.0).
    pub confidence: f64,
}

/// Detect the language of a text string.
///
/// Returns `None` if detection fails or confidence is below threshold.
/// Uses whatlang-rs trigram analysis (fast, no dictionary needed).
pub fn detect_language(text: &str) -> Option<DetectedLanguage> {
    let info = whatlang::detect(text)?;

    let (name, code) = map_whatlang_to_stemmer(info.lang())?;

    Some(DetectedLanguage {
        name,
        code,
        confidence: info.confidence(),
    })
}

/// Detect language with minimum confidence threshold.
pub fn detect_language_confident(text: &str, min_confidence: f64) -> Option<DetectedLanguage> {
    let detected = detect_language(text)?;
    if detected.confidence >= min_confidence {
        Some(detected)
    } else {
        None
    }
}

/// Map whatlang::Lang to analyzer language name + ISO code.
///
/// For CJK languages, returns the CJK analyzer name (e.g., "chinese_jieba")
/// which is handled by the `cjk` feature. For others, returns the Snowball
/// stemmer language name. Returns None for unsupported languages.
fn map_whatlang_to_stemmer(lang: whatlang::Lang) -> Option<(&'static str, &'static str)> {
    use whatlang::Lang;
    match lang {
        Lang::Ara => Some(("arabic", "ar")),
        Lang::Hye => Some(("armenian", "hy")),
        Lang::Dan => Some(("danish", "da")),
        Lang::Nld => Some(("dutch", "nl")),
        Lang::Eng => Some(("english", "en")),
        Lang::Fin => Some(("finnish", "fi")),
        Lang::Fra => Some(("french", "fr")),
        Lang::Deu => Some(("german", "de")),
        Lang::Ell => Some(("greek", "el")),
        Lang::Hun => Some(("hungarian", "hu")),
        Lang::Ita => Some(("italian", "it")),
        Lang::Nob => Some(("norwegian", "nb")),
        Lang::Por => Some(("portuguese", "pt")),
        Lang::Ron => Some(("romanian", "ro")),
        Lang::Rus => Some(("russian", "ru")),
        Lang::Spa => Some(("spanish", "es")),
        Lang::Swe => Some(("swedish", "sv")),
        Lang::Tam => Some(("tamil", "ta")),
        Lang::Tur => Some(("turkish", "tr")),
        Lang::Ukr => Some(("ukrainian", "uk")),
        // CJK languages — mapped to CJK analyzer names (per-language feature flags)
        #[cfg(feature = "cjk-zh")]
        Lang::Cmn => Some(("chinese_jieba", "zh")),
        #[cfg(feature = "cjk-ja")]
        Lang::Jpn => Some(("japanese_lindera", "ja")),
        #[cfg(feature = "cjk-ko")]
        Lang::Kor => Some(("korean_lindera", "ko")),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn detect_english() {
        let result = detect_language("This is a test of the English language detection system");
        assert!(result.is_some());
        let d = result.unwrap();
        assert_eq!(d.name, "english");
        assert_eq!(d.code, "en");
        assert!(d.confidence > 0.5);
    }

    #[test]
    fn detect_russian() {
        let result = detect_language("Это тест определения русского языка в тексте");
        assert!(result.is_some());
        let d = result.unwrap();
        assert_eq!(d.name, "russian");
        assert_eq!(d.code, "ru");
    }

    #[test]
    fn detect_ukrainian() {
        let result = detect_language(
            "Це тест визначення української мови у тексті який має бути достатньо довгим",
        );
        assert!(result.is_some());
        let d = result.unwrap();
        assert_eq!(d.name, "ukrainian");
        assert_eq!(d.code, "uk");
    }

    #[test]
    fn detect_german() {
        let result = detect_language("Dies ist ein Test der deutschen Spracherkennung im Text");
        assert!(result.is_some());
        let d = result.unwrap();
        assert_eq!(d.name, "german");
        assert_eq!(d.code, "de");
    }

    #[test]
    fn detect_french() {
        let result =
            detect_language("Ceci est un test de détection de la langue française dans le texte");
        assert!(result.is_some());
        let d = result.unwrap();
        assert_eq!(d.name, "french");
        assert_eq!(d.code, "fr");
    }

    #[test]
    fn empty_text_returns_none() {
        assert!(detect_language("").is_none());
    }

    #[test]
    fn short_text_low_confidence() {
        // Very short text may have low confidence
        let result = detect_language_confident("ok", 0.99);
        // Either None or low confidence — both acceptable
        if let Some(d) = result {
            assert!(d.confidence >= 0.99);
        }
    }

    #[test]
    fn confidence_threshold() {
        let high = detect_language_confident(
            "This is definitely English text with enough words for confident detection",
            0.8,
        );
        assert!(high.is_some(), "long English text should be confident");
    }

    // CJK language detection tests (per-language feature flags)

    #[cfg(feature = "cjk-zh")]
    #[test]
    fn detect_chinese() {
        let result = detect_language("今天天气真好我们去公园散步吧这是一个美丽的日子");
        assert!(result.is_some(), "should detect Chinese");
        let d = result.unwrap();
        assert_eq!(d.name, "chinese_jieba");
        assert_eq!(d.code, "zh");
    }

    #[cfg(feature = "cjk-ja")]
    #[test]
    fn detect_japanese() {
        let result = detect_language("東京都に住んでいます。毎日電車で会社に通っています");
        assert!(result.is_some(), "should detect Japanese");
        let d = result.unwrap();
        assert_eq!(d.name, "japanese_lindera");
        assert_eq!(d.code, "ja");
    }

    #[cfg(feature = "cjk-ko")]
    #[test]
    fn detect_korean() {
        let result = detect_language("대한민국의 수도는 서울입니다 오늘 날씨가 좋습니다");
        assert!(result.is_some(), "should detect Korean");
        let d = result.unwrap();
        assert_eq!(d.name, "korean_lindera");
        assert_eq!(d.code, "ko");
    }
}
