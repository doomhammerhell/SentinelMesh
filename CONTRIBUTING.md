# Contributing

## Expectations

- keep changes small and reviewable
- prefer Rust-first implementations for production paths
- include tests for behavioral changes
- document operator-facing changes in `README.md` or `docs/`
- avoid introducing breaking API changes without an ADR or migration note

## Development Flow

1. Fork the repository and create a branch.
2. Run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
```

3. Update documentation for any operational or architectural change.
4. Open a pull request using the template.

## Commit and PR Scope

- one concern per PR where possible
- explain user-facing and operator-facing impact
- link issues or ADRs when touching architecture or security

## Security-Sensitive Changes

Changes touching auth, signing, storage durability, canary funding or transport security should include:

- threat impact
- rollback plan
- migration steps

## Integration Tests

Tests that require `solana-test-validator` are marked `ignored`. Run them explicitly in environments where the Solana CLI toolchain is installed.
