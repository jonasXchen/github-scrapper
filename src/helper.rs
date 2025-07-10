use crate::types;

use chrono::Utc;
use reqwest::Response;
use serde_json::json;
use tokio::time::{sleep, Duration};
use types::GitHubUpdateData;
use types::RepoMap;

pub fn format_for_mapping(
    owner: &str,
    repo_name: &str,
    commit_sha: &str,
    commit_date: &str,
    keyword_counts: &RepoMap,
    email: &str,
    name: &str,
    origin: Option<&str>,
    file_types: &str,
    files_processed: &usize,
) -> GitHubUpdateData {
    let mut keyword_counts_json = serde_json::Map::new();

    for (keyword, kc) in keyword_counts {
        keyword_counts_json.insert(
            keyword.clone(),
            json!({
                "count": kc.count,
                "files": kc.files
            }),
        );
    }

    let keyword_matches = &keyword_counts_json.len();

    let snapshot_url = format!(
        "https://github.com/{}/{}/tree/{}",
        owner, repo_name, commit_sha
    );

    let origin = origin.unwrap_or("unknown");

    let formatted_result = GitHubUpdateData {
        repo_name: repo_name.to_string(),
        owner: owner.to_string(),
        commit_sha: commit_sha.to_string(),
        email: email.to_string(),
        name: name.to_string(),
        snapshot_url: snapshot_url.to_string(),
        commit_date: commit_date.to_string(),
        keyword_counts: keyword_counts.clone(),
        keyword_matches: keyword_matches.to_string(),
        origin: origin.to_string(),
        file_types: file_types.to_string(),
        files_processed: files_processed.to_string(),
    };

    formatted_result
}

pub async fn check_api_request_limit(resp: &Response) {
    if let Some(remaining) = resp.headers().get("X-RateLimit-Remaining") {
        let rem = remaining
            .to_str()
            .unwrap_or("0")
            .parse::<i32>()
            .unwrap_or(0);

        if rem <= 1 {
            if let Some(reset) = resp.headers().get("X-RateLimit-Reset") {
                let reset_ts = reset.to_str().unwrap_or("0").parse::<i64>().unwrap_or(0);
                let now = Utc::now().timestamp();
                let mut wait_time = reset_ts - now;

                if wait_time > 0 {
                    println!("⏳ Rate limit hit. Sleeping for {} seconds...", wait_time);

                    // Print progress every 60s
                    while wait_time > 0 {
                        let sleep_duration = std::cmp::min(wait_time, 60);
                        sleep(Duration::from_secs(sleep_duration as u64)).await;
                        wait_time -= sleep_duration;

                        if wait_time > 0 {
                            println!("⏳ Still waiting... {} seconds remaining", wait_time);
                        }
                    }
                }
            }
        }
    }
}
