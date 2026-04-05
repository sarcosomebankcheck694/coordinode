# Contributing to CoordiNode

We welcome contributions from everyone. This document explains how to get involved.

## Ways to Contribute

- **Bug reports** — found something broken? [Open an issue](https://github.com/structured-world/coordinode/issues/new?template=bug_report.yml)
- **Feature requests** — have an idea? [Suggest it](https://github.com/structured-world/coordinode/issues/new?template=feature_request.yml)
- **Code** — fix a bug or implement a feature (see below)
- **Documentation** — improve docs, fix typos, add examples

## Development Setup

```bash
# Clone
git clone https://github.com/structured-world/coordinode.git
cd coordinode

# Build
cargo build

# Run tests
cargo test --workspace

# Run with Clippy (must pass with zero warnings)
cargo clippy --all-targets --all-features -- -D warnings
```

Requires Rust 1.82+.

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`feat/description` or `fix/description`)
3. Make your changes
4. Ensure all checks pass:
   - `cargo check --workspace`
   - `cargo clippy --all-targets --all-features -- -D warnings` (zero warnings)
   - `cargo test --workspace` (all tests pass)
5. Write a clear commit message following [Conventional Commits](https://www.conventionalcommits.org/)
6. Open a pull request with a description of what changed and why

## Code Style

- **Rust** — follow `rustfmt` defaults and Clippy recommendations
- `pub(crate)` by default — explicit `pub` only for public API
- `Result<T, E>` everywhere — no `unwrap()` on I/O paths
- Tests for every function — happy path + error path at minimum

## Contributor License Agreement (CLA)

By submitting a pull request, you agree that your contributions are licensed under the same license as the project (AGPL-3.0-only). You retain copyright of your contributions.

We use a lightweight CLA to ensure all contributions can be distributed under the project license. The CLA bot will guide you through signing on your first PR.

## Code of Conduct

Be respectful. We don't have a formal code of conduct document, but we expect professional behavior. Harassment, discrimination, and bad faith arguments are not tolerated.

## Questions?

Open a [question issue](https://github.com/structured-world/coordinode/issues/new?template=question.yml) or reach out at oss@sw.foundation.
