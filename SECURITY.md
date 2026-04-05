# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in CoordiNode, please report it responsibly.

**Do NOT open a public issue.** Instead:

1. Email **security@sw.foundation** with:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

2. You will receive an acknowledgment within 48 hours.

3. We will work with you to understand and fix the issue before any public disclosure.

## Scope

This policy covers:
- CoordiNode CE (this repository)
- The `coordinode` binary and `coordinode-embed` library
- Docker images published by the project
- Proto definitions in the `proto/` submodule

## Supported Versions

| Version | Supported |
|---------|-----------|
| latest alpha | Yes |
| older alphas | Best effort |

## Disclosure Timeline

- **0 days** — Report received, acknowledgment sent
- **7 days** — Initial assessment and severity classification
- **30 days** — Fix developed and tested
- **45 days** — Fix released, advisory published (CVE if applicable)

We follow coordinated disclosure. We will credit reporters unless they prefer anonymity.
