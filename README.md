# integration-validation

Rust tooling that validates MagicBlock integrations by scraping GitHub repos, checking on-chain program activity, and measuring developer commit activity — with results written back to a Google Sheet (and optionally ingested into Elasticsearch).

The crate ships three binaries:

| Binary              | Source                         | What it does                                                                                                                                                                                                                                                                                          |
| ------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scraper`           | `src/main.rs`                  | Reads GitHub repo/user URLs from a sheet, scans each repo for MagicBlock SDK keywords (`ephemeral-rollups-sdk`, `#[delegate]`, etc.), writes JSON results + match counts back to the sheet, and can also run a public GitHub code search. Optionally ingests results into Elasticsearch via Logstash. |
| `integration-check` | `src/bin/integration_check.rs` | Reads a "program address" column from a sheet, fetches each program's recent Solana transactions (mainnet and/or devnet), and counts how often the MagicBlock target programs (delegation, ACL, VRF, ephemeral SPL) appear in them. One result column per target per network, plus totals.            |
| `commit-check`      | `src/bin/commit_check.rs`      | Reads a "Repo URL" column from a sheet, resolves each repo's owner, and uses the GitHub GraphQL API to write "commits in last N days" and "active days in last N days" columns.                                                                                                                       |

## Prerequisites

- Rust toolchain (stable) — install via [rustup](https://rustup.rs)
- A GitHub personal access token (repo read scope; GraphQL access for `commit-check`)
- A Google Cloud service account with the Sheets API enabled, and the target spreadsheet shared with the service account's email (Editor access)
- For `integration-check`: Solana RPC endpoint URL(s)
- For `scraper`'s Elasticsearch ingestion: an ES endpoint + API key

## Configuration

All configuration is via environment variables, loaded from a `.env` file in the repo root (via `dotenvy`).

```dotenv
# Required for all binaries
GOOGLE_SERVICE_ACCOUNT_B64=<base64-encoded service-account JSON>
SPREADSHEET_ID=<google sheet id>            # optional; defaults are hardcoded

# Required for scraper and commit-check
PRIVATE_GITHUB_TOKEN=<github token>

# Required for integration-check (at least one)
SOLANA_RPC_URL_MAINNET=<mainnet rpc url>
SOLANA_RPC_URL_DEVNET=<devnet rpc url>
# SOLANA_RPC_URL=<rpc url>                  # legacy alias, treated as mainnet

# Required for scraper's Elasticsearch ingestion
ES_INDEX=<index name>
ES_ENDPOINT=<elasticsearch endpoint>
ES_APIKEY=<elasticsearch api key>
```

To produce `GOOGLE_SERVICE_ACCOUNT_B64` from a service-account JSON file:

```bash
base64 -i service-account.json | tr -d '\n'
```

## Running

Build everything once:

```bash
cargo build --release
```

### 1. Scraper (`scraper`)

```bash
cargo run --release --bin scraper
```

By default it reads the configured sheet ("Frontier" unless overridden) and writes results into auto-resolved columns (`Scraper Result (JSON)`, `Scraper Keyword Matches`, `Scraper Snapshot URL`). Results are also saved to `results.json`.

Optional environment variables:

| Variable                                               | Default                 | Effect                                                                         |
| ------------------------------------------------------ | ----------------------- | ------------------------------------------------------------------------------ |
| `SCRAPER_MODE`                                         | `sheet`                 | `search` (GitHub code search only), `sheet` (sheet rows only), or `all` (both) |
| `RUN_SEARCH` / `RUN_SHEETS`                            | `false` / `true`        | Fine-grained toggles (overridden by `SCRAPER_MODE`)                            |
| `SCRAPER_SHEET_NAME` (or `SHEET_NAME`)                 | `Frontier`              | Sheet tab to read and write                                                    |
| `SCRAPER_READ_SHEET_NAME` / `SCRAPER_WRITE_SHEET_NAME` | sheet name above        | Split read/write tabs                                                          |
| `ROW_SKIP`                                             | `0`                     | Skip the first N data rows                                                     |
| `SCRAPER_CONTINUE`                                     | `false`                 | Skip rows that already have a result (resume mode)                             |
| `SCRAPER_CONTINUE_COLUMN`                              | `Scraper Result (JSON)` | Header used to detect already-processed rows                                   |

Example — resume a run that stopped partway through the "Frontier" tab:

```bash
SCRAPER_CONTINUE=true cargo run --release --bin scraper
```

### 2. Integration check (`integration-check`)

```bash
cargo run --release --bin integration-check
```

Reads the column whose header contains "program address" from the configured sheet, inspects each address's recent transactions on every configured network, and writes one count column per target program plus a `total` column per network.

The target program IDs live in the `TARGETS` const at the top of `src/bin/integration_check.rs` — edit that list to change what is checked.

Optional environment variables:

| Variable                      | Default                 | Effect                                                                                                 |
| ----------------------------- | ----------------------- | ------------------------------------------------------------------------------------------------------ |
| `INTEGRATION_SHEET_NAME`      | `Founders Camp (BUILD)` | Sheet tab to read/write                                                                                |
| `INTEGRATION_READ_RANGE`      | (whole sheet)           | A1-style range restriction, e.g. `A:T`                                                                 |
| `INTEGRATION_WRITE_START_COL` | (auto)                  | Force result columns to start at this letter; when empty, columns are resolved/appended by header name |
| `INTEGRATION_TX_LIMIT`        | `100`                   | Recent transactions inspected per program                                                              |
| `INTEGRATION_CONCURRENCY`     | `10`                    | Program addresses processed in parallel                                                                |
| `RPC_BATCH`                   | `true`                  | Use JSON-RPC batching; set `false` if your RPC provider rejects array bodies                           |

### 3. Commit check (`commit-check`)

```bash
cargo run --release --bin commit-check
```

Reads the "Repo URL" column from the configured sheet and writes commit-activity columns per row.

Optional environment variables:

| Variable                 | Default       | Effect                                                  |
| ------------------------ | ------------- | ------------------------------------------------------- |
| `INTEGRATION_SHEET_NAME` | `Blitz v5`    | Sheet tab to read/write (shared with integration-check) |
| `INTEGRATION_READ_RANGE` | (whole sheet) | A1-style range restriction                              |
| `COMMIT_WINDOW_DAYS`     | `30`          | Look-back window for commit activity                    |
| `COMMIT_CONCURRENCY`     | `8`           | Owners queried in parallel                              |

## GitHub Actions

The same binaries run in CI via workflows in `.github/workflows/`:

- `weekly-scrape.yml` — scheduled run of `scraper`
- `manual-sheet-scrape.yml` — manually dispatched `scraper` run
- `integration-check.yml` — runs `integration-check`
- `commit-check.yml` — runs `commit-check`

Each workflow expects the same environment variables as above, supplied as repository secrets.

## Notes

- Column positions are not hardcoded: writers resolve columns by header name and append new columns at the next empty slot, so it is safe to reorder sheet columns between runs.
- `service-account.json` and `.env` contain credentials — they are git-ignored and must never be committed.
