// Standalone GitHub commit-activity binary.
//
// Reads a "Repo URL" column from the configured sheet, then for each row:
//   1. Parses the repo owner (the GitHub login/org) from the URL.
//   2. Asks the GitHub GraphQL API on how many distinct days in the window the
//      owner authored at least one commit ("active commit-days", 0..=N).
//   3. Writes the count into an "Active days (last N days)" column.
//
// Active-days rather than raw commit count is deliberate: raw counts are
// dominated by automated/bot repos (one repo auto-committing every few seconds
// can register tens of thousands of commits a month) and by GitHub's commit-
// search fork duplication. GraphQL pre-aggregates commits into one bucket per
// (repo, day), so a day is simply active or not — immune to commit volume.
//
// Only shared infrastructure (Google Sheets helpers + GitHub URL parsing) is
// imported from the crate; everything else lives in this file so it can be
// lifted out as-is, mirroring integration_check.rs.

use anyhow::{anyhow, Result};
use dotenvy::dotenv;
use futures::{stream, StreamExt};
use google_sheets4::{api::ValueRange, Sheets};
use integration_validation::github::{classify_github_url, GitHubUrlType};
use integration_validation::sheets::{
    batch_update_values, init_sheets, read_columns_from_sheet, resolve_or_append_columns,
};
use reqwest::header::USER_AGENT;
use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::env;
use std::time::Duration;

// ─── Defaults (override via .env if desired) ─────────────────────────────────
const DEFAULT_SPREADSHEET_ID: &str = "1aYacUptAwX2bqbvy9uZFdzcVjdTB7RXmjqLo851NTxs";
const DEFAULT_SHEET_NAME: &str = "Frontier";
const DEFAULT_READ_RANGE: &str = "";
/// How far back to count commit activity, in days.
const DEFAULT_WINDOW_DAYS: i64 = 30;
/// Concurrency for the GitHub GraphQL calls. The GraphQL budget is generous
/// (~5000 points/hour), so this can be higher than the old Search API path.
const DEFAULT_CONCURRENCY: usize = 8;

/// Extract the owner/login from any GitHub URL — works for both bare account
/// URLs (`github.com/<login>`) and repo URLs (`github.com/<login>/<repo>`).
fn extract_owner(url: &str) -> Option<String> {
    match classify_github_url(url) {
        GitHubUrlType::User(owner) => Some(owner),
        GitHubUrlType::Repo { owner, .. } => Some(owner),
        GitHubUrlType::Invalid => None,
    }
}

/// POST a GraphQL query and return its `data` object. Retries transient
/// failures (5xx, secondary-rate-limit 403/429, transport errors). GraphQL
/// reports query-level problems in a top-level `errors` array even on HTTP
/// 200; those are not retryable, so we surface them and return whatever
/// `data` came back (often a partial/null result).
async fn graphql_post(http: &Client, token: &str, body: &Value) -> Option<Value> {
    let mut attempt: u32 = 0;
    loop {
        let resp = http
            .post("https://api.github.com/graphql")
            .header(USER_AGENT, "integration-validation")
            .bearer_auth(token)
            .json(body)
            .send()
            .await;

        match resp {
            Ok(r) => {
                let status = r.status();
                if status.is_success() {
                    let v: Value = r.json().await.ok()?;
                    if let Some(errs) = v.get("errors") {
                        eprintln!("⚠️  GraphQL errors: {}", errs);
                    }
                    return v.get("data").cloned();
                }
                if status.as_u16() != 403 && status.as_u16() != 429 && !status.is_server_error() {
                    eprintln!("⚠️  GraphQL HTTP {}", status);
                    return None;
                }
            }
            Err(e) => {
                if attempt >= 5 {
                    eprintln!("⚠️  GraphQL transport error: {}", e);
                    return None;
                }
            }
        }
        attempt += 1;
        if attempt > 5 {
            eprintln!("⚠️  GraphQL retry budget exhausted");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(1000 * (1u64 << attempt))).await;
    }
}

/// Count distinct days in `[from, to]` on which `owner` authored at least one
/// commit. Returns `None` only on lookup failure (not "zero days").
///
/// A repo's owner may be a person or a company/organization, so we try both:
///   - **User** → contributions collection. GitHub pre-aggregates commits into
///     one bucket per (repo, day), so the result is immune to commit volume —
///     an auto-committing bot repo still contributes a single active day.
///   - **Organization** → union of commit days across the org's non-fork repos'
///     default branches (bounded to 100 repos × 100 recent commits in window).
async fn active_commit_days(
    http: &Client,
    token: &str,
    owner: &str,
    from: &str,
    to: &str,
) -> Option<u32> {
    // ── Personal account ────────────────────────────────────────────────────
    let user_q = json!({
        "query": "query($login:String!,$from:DateTime!,$to:DateTime!){user(login:$login){contributionsCollection(from:$from,to:$to){commitContributionsByRepository(maxRepositories:100){contributions(first:100){nodes{occurredAt}}}}}}",
        "variables": { "login": owner, "from": from, "to": to }
    });
    if let Some(data) = graphql_post(http, token, &user_q).await {
        if data.get("user").map(|u| !u.is_null()).unwrap_or(false) {
            let mut days: HashSet<String> = HashSet::new();
            if let Some(repos) = data
                .pointer("/user/contributionsCollection/commitContributionsByRepository")
                .and_then(|v| v.as_array())
            {
                for repo in repos {
                    if let Some(nodes) =
                        repo.pointer("/contributions/nodes").and_then(|v| v.as_array())
                    {
                        for n in nodes {
                            if let Some(d) = n.get("occurredAt").and_then(|v| v.as_str()) {
                                days.insert(d[..10.min(d.len())].to_string());
                            }
                        }
                    }
                }
            }
            return Some(days.len() as u32);
        }
    }

    // ── Organization ────────────────────────────────────────────────────────
    let org_q = json!({
        "query": "query($login:String!,$from:GitTimestamp!){organization(login:$login){repositories(first:100,isFork:false,orderBy:{field:PUSHED_AT,direction:DESC}){nodes{defaultBranchRef{target{... on Commit{history(first:100,since:$from){nodes{committedDate}}}}}}}}}",
        "variables": { "login": owner, "from": from }
    });
    if let Some(data) = graphql_post(http, token, &org_q).await {
        if data.get("organization").map(|o| !o.is_null()).unwrap_or(false) {
            let mut days: HashSet<String> = HashSet::new();
            if let Some(repos) = data
                .pointer("/organization/repositories/nodes")
                .and_then(|v| v.as_array())
            {
                for repo in repos {
                    if let Some(nodes) = repo
                        .pointer("/defaultBranchRef/target/history/nodes")
                        .and_then(|v| v.as_array())
                    {
                        for n in nodes {
                            if let Some(d) = n.get("committedDate").and_then(|v| v.as_str()) {
                                days.insert(d[..10.min(d.len())].to_string());
                            }
                        }
                    }
                }
            }
            return Some(days.len() as u32);
        }
    }

    None
}

async fn run_check(
    sheets: &Sheets,
    http: &Client,
    github_token: &str,
    spreadsheet_id: &str,
    sheet_name: &str,
    read_range: &str,
    from: &str,
    to: &str,
    window_days: u32,
    active_header: &str,
    concurrency: usize,
) -> Result<()> {
    let columns = read_columns_from_sheet(sheets, spreadsheet_id, sheet_name, read_range).await?;

    let repo_key = columns
        .keys()
        .find(|k| k.to_lowercase().contains("repo url"))
        .or_else(|| columns.keys().find(|k| k.to_lowercase().contains("repo")))
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "No column containing 'repo' in sheet '{}' range {}",
                sheet_name,
                read_range
            )
        })?;
    println!("Using column '{}' for GitHub repo URLs.", repo_key);
    let repos = columns.get(&repo_key).cloned().unwrap_or_default();

    // Plural resolver expands the grid (via ensure_grid_columns) when the new
    // header would land past the sheet's current column edge.
    let active_col = resolve_or_append_columns(
        sheets,
        spreadsheet_id,
        sheet_name,
        &[active_header.to_string()],
    )
    .await?
    .into_iter()
    .next()
    .ok_or_else(|| anyhow!("failed to resolve '{}' column", active_header))?;
    println!(
        "Writing '{}' results to column {}.",
        active_header, active_col
    );

    // Row index in the sheet is +2: row 1 is the header, and `repos` starts at
    // the first data row.
    let work: Vec<(usize, String)> = repos
        .into_iter()
        .enumerate()
        .map(|(i, r)| (i + 2, r.trim().to_string()))
        .filter(|(_, r)| !r.is_empty())
        .collect();

    println!(
        "Counting active commit-days in [{}, {}] for {} repo owner(s) at concurrency {}...",
        from,
        to,
        work.len(),
        concurrency
    );

    // Stream results as each row completes (up to `concurrency` in flight) and
    // flush to the sheet every FLUSH_EVERY rows, so a mid-run crash keeps the
    // rows already written instead of losing the whole batch. Rows arrive out
    // of order, but each write targets an absolute cell range, so order doesn't
    // matter. Flushing every ~50 rows stays far under the 60/min Sheets quota.
    const FLUSH_EVERY: usize = 50;
    let total = work.len();

    let stream = stream::iter(work)
        .map(|(row, repo_url)| async move {
            let count = match extract_owner(&repo_url) {
                // Clamp to the window: inclusive day boundaries can yield N+1.
                Some(owner) => active_commit_days(http, github_token, &owner, from, to)
                    .await
                    .map(|d| d.min(window_days)),
                None => {
                    eprintln!("⚠️  invalid GitHub URL on row {}: {}", row, repo_url);
                    None
                }
            };
            (row, repo_url, count)
        })
        .buffer_unordered(concurrency);
    futures::pin_mut!(stream);

    let mut pending: Vec<ValueRange> = Vec::new();
    let mut processed = 0usize;
    let mut written = 0usize;

    while let Some((row, repo_url, count)) = stream.next().await {
        let owner = extract_owner(&repo_url).unwrap_or_else(|| "?".to_string());
        let val = count.map(|c| c.to_string()).unwrap_or_default();
        processed += 1;
        println!(
            "  [{:>4}/{}] row {:>4} {:>24} → {}",
            processed,
            total,
            row,
            owner,
            count
                .map(|c| c.to_string())
                .unwrap_or_else(|| "—".to_string())
        );
        pending.push(ValueRange {
            range: Some(format!("'{}'!{}{}", sheet_name, active_col, row)),
            values: Some(vec![vec![val]]),
            major_dimension: Some("ROWS".to_string()),
            ..Default::default()
        });

        if pending.len() >= FLUSH_EVERY {
            let n = pending.len();
            batch_update_values(sheets, spreadsheet_id, std::mem::take(&mut pending)).await?;
            written += n;
            println!("  ✅ flushed {} row(s) ({}/{} written so far)", n, written, total);
        }
    }

    // Final flush for the remainder.
    if !pending.is_empty() {
        let n = pending.len();
        batch_update_values(sheets, spreadsheet_id, std::mem::take(&mut pending)).await?;
        written += n;
        println!("  ✅ flushed final {} row(s) ({}/{} total)", n, written, total);
    }

    Ok(())
}

// ─── Entry point ─────────────────────────────────────────────────────────────
#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let github_token = env::var("PRIVATE_GITHUB_TOKEN")
        .map_err(|_| anyhow!("Missing PRIVATE_GITHUB_TOKEN env var"))?;

    let spreadsheet_id =
        env::var("SPREADSHEET_ID").unwrap_or_else(|_| DEFAULT_SPREADSHEET_ID.to_string());
    let sheet_name =
        env::var("INTEGRATION_SHEET_NAME").unwrap_or_else(|_| DEFAULT_SHEET_NAME.to_string());
    let read_range =
        env::var("INTEGRATION_READ_RANGE").unwrap_or_else(|_| DEFAULT_READ_RANGE.to_string());
    let window_days: i64 = env::var("COMMIT_WINDOW_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_WINDOW_DAYS);
    let concurrency: usize = env::var("COMMIT_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CONCURRENCY);

    let now = chrono::Utc::now();
    let to = now.to_rfc3339();
    let from = (now - chrono::Duration::days(window_days)).to_rfc3339();
    // Header tracks the actual window, so a different window lands in its own
    // column instead of overwriting/mislabeling another run's results.
    let active_header = format!("Active days (last {} days)", window_days);

    println!(
        "Active commit-days check on sheet '{}' (range {}), window {} day(s) ([{}, {}]).",
        sheet_name, read_range, window_days, from, to
    );

    let sheets = init_sheets().await?;
    let http = Client::new();
    println!("Initialized Sheets + GitHub clients.");

    run_check(
        &sheets,
        &http,
        &github_token,
        &spreadsheet_id,
        &sheet_name,
        &read_range,
        &from,
        &to,
        window_days as u32,
        &active_header,
        concurrency,
    )
    .await?;

    println!("✅ Active commit-days check complete.");
    Ok(())
}
