# Transaction engine

- Execute with `cargo run -- <csv.file>`
- Run tests with `cargo test`

## Assumptions
- Deposit and withdraw actions are skipped
- Dispute, Resolve and chargeback actions cannot be in dispute themselves
- When a Withdrawal transaction is in dispute the amount will be added to available funds and subtracted from held funds.
- When a Deposit transaction is in dispute the amount will be subtracted from available funds and added to held funds.
