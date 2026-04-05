---
applyTo: "**/*.rs"
---

# Code Review Instructions for CoordiNode

## Scope Rules (CRITICAL)

- **Review ONLY code within the PR's diff.** Do not suggest inline fixes for unchanged lines.
- For issues in code **outside the diff**, suggest creating a **separate issue** instead of proposing code changes. Example: "Consider opening an issue to add validation here — this is outside this PR's scope."
- **Read the PR description carefully.** If the PR body has an "out of scope" section listing items handled by other PRs, do NOT flag those items.

## Rust Standards

- `unsafe` blocks require `// SAFETY:` comments explaining the invariant (should be zero — `#[forbid(unsafe_code)]`)
- Prefer `#[expect(lint)]` over `#[allow(lint)]` — `#[expect]` warns when suppression becomes unnecessary
- Use `TryFrom`/`TryInto` for fallible conversions; `as` casts need `#[expect(clippy::cast_possible_truncation)]` with reason
- No `unwrap()` / `expect()` on I/O paths — use `Result` propagation
- `expect()` is acceptable for programmer invariants (lock poisoning) with `#[expect(clippy::expect_used, reason = "...")]`
- Code must pass `cargo clippy --all-features -- -D warnings`

## Testing

- Use `tempfile::tempdir()` for test directories — ensures cleanup even on panic
- Test naming: `fn <what>_<condition>_<expected>()` or `fn test_<scenario>()`
- Gold standard tests (`compound_queries.rs`) must never be weakened — if they fail, fix the code, not the test
- Prefer `assert_eq!` with message over bare `assert!` for better failure output
