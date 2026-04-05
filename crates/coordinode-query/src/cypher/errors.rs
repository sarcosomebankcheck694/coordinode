//! Parse error types for the Cypher parser.

use std::fmt;

/// Error produced during Cypher parsing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
    /// Grammar-level parse failure from pest.
    #[error("syntax error: {message}")]
    Syntax { message: String, span: ErrorSpan },

    /// AST construction failure (valid grammar but invalid semantics).
    #[error("invalid query: {0}")]
    Invalid(String),
}

/// Location information for parse errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorSpan {
    /// Byte offset of the start of the error.
    pub start: usize,
    /// Byte offset of the end of the error.
    pub end: usize,
    /// Line number (1-based).
    pub line: usize,
    /// Column number (1-based).
    pub col: usize,
}

impl fmt::Display for ErrorSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}:{}", self.line, self.col)
    }
}
