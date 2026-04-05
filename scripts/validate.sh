#!/usr/bin/env bash
# CoordiNode pre-push validation script.
#
# Runs the same checks as CI to catch issues before pushing.
# Exit on first failure.
#
# Usage:
#   ./scripts/validate.sh          # full validation
#   ./scripts/validate.sh --quick  # skip release build and audit

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() {
    echo -e "\n${YELLOW}━━━ $1 ━━━${NC}"
}

ok() {
    echo -e "${GREEN}✓ $1${NC}"
}

fail() {
    echo -e "${RED}✗ $1${NC}"
    exit 1
}

QUICK=false
if [[ "${1:-}" == "--quick" ]]; then
    QUICK=true
fi

step "Step 1/6: Format check"
cargo fmt --all -- --check || fail "Formatting issues found. Run: cargo fmt --all"
ok "Format clean"

step "Step 2/6: Clippy (zero warnings)"
cargo clippy --all-targets --all-features -- -D warnings || fail "Clippy warnings found"
ok "Clippy clean"

step "Step 3/6: Unit tests"
cargo test --lib --all-features || fail "Unit tests failed"
ok "Unit tests passed"

step "Step 4/6: Integration tests"
cargo test --test integration -p coordinode-embed --all-features || fail "Integration tests failed"
ok "Integration tests passed"

if [[ "$QUICK" == "false" ]]; then
    step "Step 5/6: Release build"
    cargo build --release --all-features || fail "Release build failed"
    ok "Release build succeeded"

    step "Step 6/6: Security audit"
    if command -v cargo-audit &> /dev/null; then
        cargo audit || fail "Security vulnerabilities found"
        ok "Audit clean"
    else
        echo -e "${YELLOW}⚠ cargo-audit not installed, skipping. Install: cargo install cargo-audit${NC}"
    fi
else
    echo -e "${YELLOW}⚠ Skipping release build and audit (--quick mode)${NC}"
fi

echo -e "\n${GREEN}━━━ All checks passed ━━━${NC}"
