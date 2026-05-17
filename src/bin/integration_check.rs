// Standalone integration-validation binary.
//
// Reads the "program address" column from the configured sheet, then for each
// row's source program A:
//   1. Fetches A's last N transaction signatures.
//   2. Batch-fetches the full transactions.
//   3. For each transaction, checks whether each TARGET pubkey appears in the
//      account list (legacy accountKeys + v0 loadedAddresses). One column per
//      target receives the count.
//
// This gives complete coverage of A's history regardless of how busy the
// targets are — the inverse trade-off vs naive sig-set intersection.
//
// Only shared infrastructure (Google Sheets helpers) is imported from the
// crate; everything else lives in this file so it can be lifted out as-is.

use anyhow::{anyhow, Result};
use chrono::DateTime;
use dotenvy::dotenv;
use futures::{stream, StreamExt};
use google_sheets4::{api::ValueRange, Sheets};
use integration_validation::sheets::{
    batch_update_values, column_letter_to_number, column_number_to_letter, init_sheets,
    read_columns_from_sheet, resolve_or_append_columns,
};
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ─── Programs to check against ───────────────────────────────────────────────
// Edit this list to set which programs (A, B, C, …) every row's program
// address is checked against. Each entry produces its own result column.
const TARGETS: &[&str] = &[
    "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh",
    "ACLseoPoyC3cBqoUtkbjZ4aDrkurZW86v19pXz2XQnp1",
    "Vrf1RNUjXmQGjmQrQLvJHs9SNkvDJEsRVFPkfSQUwGz",
];

// ─── Defaults (override via .env if desired) ─────────────────────────────────
const DEFAULT_SPREADSHEET_ID: &str = "1aYacUptAwX2bqbvy9uZFdzcVjdTB7RXmjqLo851NTxs";
const DEFAULT_SHEET_NAME: &str = "SNS Frontier Privacy Track";
const DEFAULT_READ_RANGE: &str = "";
const DEFAULT_WRITE_START_COL: &str = "";
/// Maximum number of recent transactions to inspect for each source program.
const DEFAULT_TX_LIMIT: usize = 100;
const DEFAULT_CONCURRENCY: usize = 10;
/// Whether to send `getTransaction` calls as JSON-RPC batches. Default on.
/// Disable via `RPC_BATCH=false` if your RPC provider rejects array bodies.
const DEFAULT_RPC_BATCH: bool = true;
/// Max JSON-RPC sub-requests per batched POST.
const BATCH_SIZE: usize = 100;

// ─── Signature scan result (sigs + time window) ──────────────────────────────
struct SigScan {
    sigs: HashSet<String>,
    /// Unix seconds of the oldest signature seen (= furthest back we looked).
    oldest_bt: Option<i64>,
    /// Unix seconds of the newest signature seen (= most recent activity).
    newest_bt: Option<i64>,
}

// ─── RPC client ──────────────────────────────────────────────────────────────
struct SolanaClient {
    url: String,
    http: Client,
    id: AtomicU64,
}

impl SolanaClient {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            http: Client::new(),
            id: AtomicU64::new(1),
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let mut attempt: u32 = 0;
        loop {
            match self.http.post(&self.url).json(&body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let v: Value = resp.json().await?;
                        if let Some(err) = v.get("error") {
                            return Err(anyhow!("RPC error from {}: {}", method, err));
                        }
                        return Ok(v);
                    }
                    if status.as_u16() != 429 && !status.is_server_error() {
                        return Err(anyhow!("RPC HTTP {} from {}", status, method));
                    }
                }
                Err(e) => {
                    if attempt >= 5 {
                        return Err(anyhow!("RPC transport error: {}", e));
                    }
                }
            }
            attempt += 1;
            if attempt > 5 {
                return Err(anyhow!("RPC retry budget exhausted for {}", method));
            }
            tokio::time::sleep(Duration::from_millis(250 * (1u64 << attempt))).await;
        }
    }

    /// Issue many JSON-RPC calls in one POST as an array body. Per the spec
    /// responses can come back in any order; we sort by id before returning.
    async fn batch_rpc(&self, calls: Vec<(String, Value)>) -> Result<Vec<Value>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let body: Value = Value::Array(
            calls
                .iter()
                .enumerate()
                .map(|(i, (method, params))| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": i + 1,
                        "method": method,
                        "params": params,
                    })
                })
                .collect(),
        );

        let mut attempt: u32 = 0;
        loop {
            match self.http.post(&self.url).json(&body).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        let v: Value = resp.json().await?;
                        let mut arr = v
                            .as_array()
                            .ok_or_else(|| anyhow!("batch response was not a JSON array"))?
                            .clone();
                        arr.sort_by_key(|item| {
                            item.get("id").and_then(|i| i.as_u64()).unwrap_or(0)
                        });
                        return Ok(arr);
                    }
                    if status.as_u16() != 429 && !status.is_server_error() {
                        return Err(anyhow!("batch RPC HTTP {}", status));
                    }
                }
                Err(e) => {
                    if attempt >= 5 {
                        return Err(anyhow!("batch RPC transport error: {}", e));
                    }
                }
            }
            attempt += 1;
            if attempt > 5 {
                return Err(anyhow!("batch RPC retry budget exhausted"));
            }
            tokio::time::sleep(Duration::from_millis(250 * (1u64 << attempt))).await;
        }
    }

    /// Fetch full transactions for many signatures. Returns one entry per
    /// input signature in the same order; `None` means that tx fetch failed
    /// (skip that tx in counting). If `batch` is true uses JSON-RPC batching
    /// for ~10-50× fewer HTTP requests.
    async fn fetch_transactions(&self, sigs: &[String], batch: bool) -> Result<Vec<Option<Value>>> {
        let mut out: Vec<Option<Value>> = Vec::with_capacity(sigs.len());
        let cfg = json!({
            "encoding": "json",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed",
        });

        if batch {
            for chunk in sigs.chunks(BATCH_SIZE) {
                let calls: Vec<(String, Value)> = chunk
                    .iter()
                    .map(|sig| ("getTransaction".to_string(), json!([sig, cfg.clone()])))
                    .collect();
                let responses = self.batch_rpc(calls).await?;
                // Length should match chunk length; if RPC dropped entries we
                // fill the gap with None to keep indices aligned.
                for i in 0..chunk.len() {
                    let entry = responses.get(i).cloned();
                    let tx = entry.and_then(|e| {
                        if e.get("error").is_some() {
                            None
                        } else {
                            e.get("result").cloned()
                        }
                    });
                    out.push(tx);
                }
            }
        } else {
            for sig in sigs {
                let res = self.rpc("getTransaction", json!([sig, cfg.clone()])).await;
                match res {
                    Ok(v) => out.push(v.get("result").cloned()),
                    Err(_) => out.push(None),
                }
            }
        }
        Ok(out)
    }

    async fn fetch_signatures(&self, address: &str, max_total: usize) -> Result<SigScan> {
        let mut all: HashSet<String> = HashSet::new();
        let mut newest_bt: Option<i64> = None;
        let mut oldest_bt: Option<i64> = None;
        let mut before: Option<String> = None;

        while all.len() < max_total {
            let limit = (max_total - all.len()).min(1000).max(1);
            let mut cfg = json!({ "limit": limit });
            if let Some(b) = &before {
                cfg["before"] = Value::String(b.clone());
            }
            let resp = self
                .rpc("getSignaturesForAddress", json!([address, cfg]))
                .await?;
            let arr = resp
                .get("result")
                .and_then(|r| r.as_array())
                .cloned()
                .unwrap_or_default();
            if arr.is_empty() {
                break;
            }

            let arr_len = arr.len();
            let mut last_sig: Option<String> = None;
            for s in arr {
                if let Some(bt) = s.get("blockTime").and_then(|v| v.as_i64()) {
                    newest_bt = Some(newest_bt.map_or(bt, |n| n.max(bt)));
                    oldest_bt = Some(oldest_bt.map_or(bt, |o| o.min(bt)));
                }
                if let Some(sig) = s.get("signature").and_then(|v| v.as_str()) {
                    last_sig = Some(sig.to_string());
                    all.insert(sig.to_string());
                }
            }

            if arr_len < limit {
                break;
            }
            match last_sig {
                Some(s) => before = Some(s),
                None => break,
            }
        }
        Ok(SigScan {
            sigs: all,
            oldest_bt,
            newest_bt,
        })
    }
}

// ─── Network bundle (name + RPC client) ──────────────────────────────────────
struct Network {
    name: String,
    client: Arc<SolanaClient>,
}

// ─── Per-network result for one source program ───────────────────────────────
struct NetworkResult {
    counts: Vec<usize>,
    txs_scanned: usize,
    oldest_bt: Option<i64>,
    newest_bt: Option<i64>,
}

impl NetworkResult {
    fn empty(num_targets: usize) -> Self {
        Self {
            counts: vec![0; num_targets],
            txs_scanned: 0,
            oldest_bt: None,
            newest_bt: None,
        }
    }
}

/// Inspect one source program on one network: fetch its recent sigs, fetch
/// the txs, count target appearances in account lists.
async fn inspect_for_network(
    client: &SolanaClient,
    prog_a: &str,
    tx_limit: usize,
    targets: &[String],
    use_batch: bool,
) -> NetworkResult {
    let scan = match client.fetch_signatures(prog_a, tx_limit).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("⚠️  sigs fetch failed for {}: {}", prog_a, e);
            return NetworkResult::empty(targets.len());
        }
    };
    let sig_list: Vec<String> = scan.sigs.iter().cloned().collect();

    let mut counts = vec![0usize; targets.len()];
    let mut txs_scanned = 0usize;
    if !sig_list.is_empty() {
        match client.fetch_transactions(&sig_list, use_batch).await {
            Ok(txs) => {
                for tx_opt in txs {
                    let Some(tx) = tx_opt else { continue };
                    txs_scanned += 1;
                    let accs = extract_accounts(&tx);
                    for (i, t) in targets.iter().enumerate() {
                        if accs.contains(t) {
                            counts[i] += 1;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("⚠️  tx fetch failed for {}: {}", prog_a, e);
            }
        }
    }
    NetworkResult {
        counts,
        txs_scanned,
        oldest_bt: scan.oldest_bt,
        newest_bt: scan.newest_bt,
    }
}

// ─── Check ───────────────────────────────────────────────────────────────────
#[allow(clippy::too_many_arguments)]
async fn run_check(
    sheets: &Sheets,
    networks: Arc<Vec<Network>>,
    spreadsheet_id: &str,
    sheet_name: &str,
    read_range: &str,
    write_start_col: &str,
    targets: &[&str],
    tx_limit: usize,
    concurrency: usize,
    use_batch: bool,
) -> Result<()> {
    let columns = read_columns_from_sheet(sheets, spreadsheet_id, sheet_name, read_range).await?;
    let pa_key = columns
        .keys()
        .find(|k| k.to_lowercase().contains("program address"))
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "No column containing 'program address' in sheet '{}' range {}",
                sheet_name,
                read_range
            )
        })?;
    println!("Using column '{}' for program addresses.", pa_key);
    let programs = columns.get(&pa_key).cloned().unwrap_or_default();

    // Per network we write one column per target plus a trailing "total" column.
    let cols_per_row = networks.len() * (targets.len() + 1);

    // Headers, grouped by network. Each network block is
    // [net:T1, net:T2, …, net:total]. Networks appear in declared order
    // (mainnet first, then devnet).
    let header_values: Vec<String> = networks
        .iter()
        .flat_map(|n| {
            let per_target = targets.iter().map(move |t| format!("{}:{}", n.name, t));
            let total = std::iter::once(format!("{}:total", n.name));
            per_target.chain(total)
        })
        .collect();

    // Resolve one column letter per header. With write_start_col empty, each
    // header is matched independently — existing columns are reused as-is,
    // missing ones get appended at the next empty column. With an explicit
    // start letter the headers are laid out contiguously starting there
    // (and re-written) for backward compatibility.
    let result_cols: Vec<String> = if write_start_col.trim().is_empty() {
        resolve_or_append_columns(sheets, spreadsheet_id, sheet_name, &header_values).await?
    } else {
        let start = column_letter_to_number(write_start_col);
        let cols: Vec<String> = (0..header_values.len())
            .map(|i| column_number_to_letter(start + i))
            .collect();
        let end_col = column_number_to_letter(start + header_values.len().saturating_sub(1));
        batch_update_values(
            sheets,
            spreadsheet_id,
            vec![ValueRange {
                range: Some(format!(
                    "'{}'!{}1:{}1",
                    sheet_name, write_start_col, end_col
                )),
                values: Some(vec![header_values.clone()]),
                major_dimension: Some("ROWS".to_string()),
                ..Default::default()
            }],
        )
        .await?;
        cols
    };
    println!(
        "Writing integration results to columns {:?} (total {}).",
        result_cols, cols_per_row
    );

    let mut updates: Vec<ValueRange> = Vec::new();

    let work: Vec<(usize, String)> = programs
        .into_iter()
        .enumerate()
        .filter(|(_, p)| !p.trim().is_empty())
        .map(|(i, p)| (i + 2, p.trim().to_string()))
        .collect();

    println!(
        "Inspecting last {} tx(s) of {} program address(es) across {} network(s) at concurrency {} (batch={})...",
        tx_limit,
        work.len(),
        networks.len(),
        concurrency,
        use_batch
    );

    let targets_arc: Arc<Vec<String>> = Arc::new(targets.iter().map(|s| s.to_string()).collect());

    let results: Vec<(usize, String, Vec<NetworkResult>)> = stream::iter(work)
        .map(|(row, prog_a)| {
            let networks = Arc::clone(&networks);
            let targets = Arc::clone(&targets_arc);
            async move {
                // All networks for this source run in parallel.
                let futures = networks.iter().map(|net| {
                    inspect_for_network(&net.client, &prog_a, tx_limit, &targets, use_batch)
                });
                let net_results = futures::future::join_all(futures).await;
                (row, prog_a, net_results)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    let mut rows = results;
    rows.sort_by_key(|(r, _, _)| *r);

    // Per-row writes + one log line per network.
    for (row, prog_a, net_results) in &rows {
        // Values grouped by network: [net1:T1, net1:T2, …, net1:total,
        //                             net2:T1, net2:T2, …, net2:total].
        let mut row_values: Vec<String> = Vec::with_capacity(cols_per_row);
        for net_result in net_results {
            for count in &net_result.counts {
                row_values.push(count.to_string());
            }
            let total: usize = net_result.counts.iter().sum();
            row_values.push(total.to_string());
        }
        // One ValueRange per cell — columns may be non-contiguous since each
        // header was resolved independently. batch_update_values still folds
        // these into a single API call.
        for (col, val) in result_cols.iter().zip(row_values.iter()) {
            updates.push(ValueRange {
                range: Some(format!("'{}'!{}{}", sheet_name, col, row)),
                values: Some(vec![vec![val.clone()]]),
                major_dimension: Some("ROWS".to_string()),
                ..Default::default()
            });
        }

        for (net, result) in networks.iter().zip(net_results.iter()) {
            let sum: usize = result.counts.iter().sum();
            let breakdown: Vec<String> = targets
                .iter()
                .zip(result.counts.iter())
                .map(|(t, c)| format!("{}={}", short(t), c))
                .collect();
            println!(
                "  row {:>4} [{:>7}] {} → total {} [{}]  ({} txs, {} → {})",
                row,
                net.name,
                prog_a,
                sum,
                breakdown.join(", "),
                result.txs_scanned,
                fmt_date(result.oldest_bt),
                fmt_date(result.newest_bt),
            );
        }
    }

    // Flush all writes in chunks of 200 ranges per request. Each chunk
    // counts as ONE write against the 60/min/user quota.
    const CHUNK: usize = 200;
    let total_ranges = updates.len();
    let total_batches = (total_ranges + CHUNK - 1) / CHUNK;
    println!(
        "Flushing {} range(s) across {} batch request(s)...",
        total_ranges, total_batches
    );
    for (i, chunk) in updates.chunks(CHUNK).enumerate() {
        batch_update_values(sheets, spreadsheet_id, chunk.to_vec()).await?;
        println!(
            "  ✅ batch {}/{} flushed ({} range(s))",
            i + 1,
            total_batches,
            chunk.len()
        );
    }

    Ok(())
}

/// Collect every account pubkey referenced by a transaction.
///
/// Covers both legacy and v0 (versioned) txs:
///   - `transaction.message.accountKeys`          — static keys
///   - `meta.loadedAddresses.writable / readonly` — keys resolved from ALTs
fn extract_accounts(tx: &Value) -> HashSet<String> {
    let mut accs = HashSet::new();
    let paths = [
        "/transaction/message/accountKeys",
        "/meta/loadedAddresses/writable",
        "/meta/loadedAddresses/readonly",
    ];
    for p in paths {
        if let Some(arr) = tx.pointer(p).and_then(|v| v.as_array()) {
            for v in arr {
                if let Some(s) = v.as_str() {
                    accs.insert(s.to_string());
                }
            }
        }
    }
    accs
}

fn short(addr: &str) -> String {
    if addr.len() <= 6 {
        return addr.to_string();
    }
    format!("{}…{}", &addr[..3], &addr[addr.len() - 3..])
}

fn fmt_date(bt: Option<i64>) -> String {
    bt.and_then(|t| DateTime::from_timestamp(t, 0))
        .map(|d| d.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "?".to_string())
}

// ─── Entry point ─────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // Build the list of networks to check. Each entry must have an RPC URL
    // set in .env. `SOLANA_RPC_URL` (no suffix) is treated as mainnet for
    // backward compatibility.
    let network_envs: &[(&str, &[&str])] = &[
        ("mainnet", &["SOLANA_RPC_URL_MAINNET", "SOLANA_RPC_URL"]),
        ("devnet", &["SOLANA_RPC_URL_DEVNET"]),
    ];
    let mut networks: Vec<Network> = Vec::new();
    for (name, env_vars) in network_envs {
        let url = env_vars.iter().find_map(|v| env::var(v).ok());
        if let Some(url) = url {
            networks.push(Network {
                name: (*name).to_string(),
                client: Arc::new(SolanaClient::new(&url)),
            });
            println!("  {} ← {} (from {})", name, "ok", env_vars.join("/"));
        }
    }
    if networks.is_empty() {
        return Err(anyhow!(
            "No RPC endpoints configured. Set at least one of \
             SOLANA_RPC_URL_MAINNET, SOLANA_RPC_URL_DEVNET, or SOLANA_RPC_URL in .env."
        ));
    }
    let networks = Arc::new(networks);

    let spreadsheet_id =
        env::var("SPREADSHEET_ID").unwrap_or_else(|_| DEFAULT_SPREADSHEET_ID.to_string());
    let sheet_name =
        env::var("INTEGRATION_SHEET_NAME").unwrap_or_else(|_| DEFAULT_SHEET_NAME.to_string());
    let read_range =
        env::var("INTEGRATION_READ_RANGE").unwrap_or_else(|_| DEFAULT_READ_RANGE.to_string());
    let write_start_col = env::var("INTEGRATION_WRITE_START_COL")
        .unwrap_or_else(|_| DEFAULT_WRITE_START_COL.to_string());
    let tx_limit: usize = env::var("INTEGRATION_TX_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TX_LIMIT);
    let concurrency: usize = env::var("INTEGRATION_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);
    let use_batch: bool = env::var("RPC_BATCH")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(DEFAULT_RPC_BATCH);

    if TARGETS.is_empty() {
        return Err(anyhow!(
            "TARGETS const is empty — edit src/bin/integration_check.rs"
        ));
    }
    if TARGETS.iter().any(|t| t.starts_with("REPLACE_WITH_")) {
        return Err(anyhow!(
            "TARGETS still contains placeholder pubkeys — edit src/bin/integration_check.rs"
        ));
    }

    println!(
        "Integration check on sheet '{}' (range {}), writing from col {}.",
        sheet_name, read_range, write_start_col
    );
    println!("Targets: {:?}", TARGETS);

    let sheets = init_sheets().await?;
    println!(
        "Initialized Sheets client. Networks: {:?}",
        networks.iter().map(|n| n.name.as_str()).collect::<Vec<_>>()
    );

    run_check(
        &sheets,
        Arc::clone(&networks),
        &spreadsheet_id,
        &sheet_name,
        &read_range,
        &write_start_col,
        TARGETS,
        tx_limit,
        concurrency,
        use_batch,
    )
    .await?;

    println!("✅ Integration check complete.");
    Ok(())
}
