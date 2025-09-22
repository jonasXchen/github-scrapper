use crate::{
    elk::ingest_via_logstash,
    helper::{check_api_request_limit, format_for_mapping},
    types::{self, GitHubUpdateData},
};
use anyhow::Result;
use base64::{engine::general_purpose, Engine as _};
use reqwest::{
    header::{AUTHORIZATION, USER_AGENT},
    Client,
};

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use types::KeywordResult;
use url::Url;

#[derive(Debug, Deserialize)]
struct TreeItem {
    path: String,
    #[serde(rename = "type")]
    item_type: String,
}

#[derive(Debug, Deserialize)]
struct TreeResponse {
    tree: Vec<TreeItem>,
}

#[derive(Debug, Deserialize)]
struct ContentResponse {
    content: String,
    path: String,
}

pub enum GitHubUrlType {
    User(String),
    Repo { owner: String, repo_name: String },
    Invalid,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GitHubSearchResponse {
    pub items: Vec<GitHubCodeItem>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GitHubCodeItem {
    pub repository: Repository,
    pub html_url: String,
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Repository {
    pub full_name: String,
    pub html_url: String,
}

pub fn classify_github_url(url: &str) -> GitHubUrlType {
    if let Ok(parsed_url) = Url::parse(url) {
        let segments: Vec<_> = parsed_url
            .path_segments()
            .map(|c| c.collect::<Vec<_>>())
            .unwrap_or_default();

        match segments.as_slice() {
            [owner] => GitHubUrlType::User(owner.to_string()),
            [owner, repo_name] => GitHubUrlType::Repo {
                owner: owner.to_string(),
                repo_name: repo_name.to_string(),
            },
            _ => GitHubUrlType::Invalid,
        }
    } else {
        GitHubUrlType::Invalid
    }
}

pub fn get_github_repo(url: &str) -> Option<String> {
    parse_github_url(url).map(|(owner, repo)| format!("https://github.com/{}/{}", owner, repo))
}

pub fn parse_github_url(url: &str) -> Option<(String, String)> {
    Url::parse(url)
        .ok()?
        .path_segments()?
        .collect::<Vec<_>>()
        .get(0..2)
        .map(|s| (s[0].to_string(), s[1].to_string()))
}

pub async fn process_repo(
    client: &Client,
    owner: &str,
    repo: &str,
    github_token: &str,
    keywords: &[&str; 8],
    allowed_extensions: &[&str; 4],
    limit: usize,
) -> Option<(HashMap<String, KeywordResult>, String, usize)> {
    let tree_url = format!(
        "https://api.github.com/repos/{}/{}/git/trees/HEAD?recursive=1",
        owner, repo
    );

    let tree_resp = client
        .get(&tree_url)
        .header(USER_AGENT, "rusty")
        .header(AUTHORIZATION, format!("token {}", github_token))
        .send()
        .await
        .ok()?;
    check_api_request_limit(&tree_resp).await;

    // Read the response body as text
    let body = tree_resp.text().await.ok()?;

    // Parse into your struct
    let tree: TreeResponse = serde_json::from_str(&body).ok()?;

    let files: Vec<_> = tree
        .tree
        .into_iter()
        .filter(|i| i.item_type == "blob" && allowed_extensions.iter().any(|e| i.path.ends_with(e)))
        .take(limit)
        .collect(); // Debug limit

    let files_processed = files.len();

    println!(
        "📁 Number of matching files: {} for {}",
        files.len(),
        tree_url
    );

    let mut results = HashMap::new();

    for item in files {
        let file_url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}",
            owner, repo, item.path
        );

        let file_resp = client
            .get(&file_url)
            .header(USER_AGENT, "rust-scraper")
            .header(AUTHORIZATION, format!("token {}", github_token))
            .send()
            .await
            .ok()?;
        check_api_request_limit(&file_resp).await;

        let file: ContentResponse = file_resp.json().await.ok()?;
        let decoded = general_purpose::STANDARD
            .decode(file.content.replace('\n', ""))
            .ok()?;
        let text = String::from_utf8_lossy(&decoded).to_lowercase();

        for &kw in keywords {
            let count = text.matches(kw).count();
            if count > 0 {
                let entry = results.entry(kw.to_string()).or_insert(KeywordResult {
                    count: 0,
                    files: vec![],
                });
                entry.count += count;
                entry.files.push(format!(
                    "https://github.com/{}/{}/blob/HEAD/{}",
                    owner, repo, file.path
                ));
            }
        }
    }

    Some((results, allowed_extensions[..].join(", "), files_processed))
}

pub async fn get_last_commit_info(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    github_token: &str,
) -> Option<(String, String, String, String)> {
    // Step 1: Get repo metadata to find default branch
    let repo_url = format!("https://api.github.com/repos/{}/{}", owner, repo);
    let repo_resp = client
        .get(&repo_url)
        .header("User-Agent", "rust-app")
        .bearer_auth(github_token)
        .send()
        .await
        .ok()?;
    check_api_request_limit(&repo_resp).await;

    let repo_json: serde_json::Value = repo_resp.json().await.ok()?;
    let default_branch = repo_json["default_branch"].as_str()?.to_string();

    // Step 2: Get the latest commit on the default branch
    let commit_url = format!(
        "https://api.github.com/repos/{}/{}/commits/{}",
        owner, repo, default_branch
    );

    let commit_resp = client
        .get(&commit_url)
        .header("User-Agent", "rust-app")
        .bearer_auth(github_token)
        .send()
        .await
        .ok()?;
    check_api_request_limit(&commit_resp).await;

    let commit_json: serde_json::Value = commit_resp.json().await.ok()?;

    let sha = commit_json["sha"].as_str()?.to_string();
    let date = commit_json["commit"]["author"]["date"]
        .as_str()?
        .to_string();
    let email = commit_json["commit"]["author"]["email"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();
    let name = commit_json["commit"]["author"]["name"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    Some((sha, date, email, name))
}

pub async fn fetch_user_repos(
    client: &Client,
    username: &str,
    github_token: &str,
) -> (Vec<String>, usize) {
    let mut repo_urls = Vec::new();
    let mut page = 1;

    loop {
        let url = format!(
            "https://api.github.com/users/{}/repos?per_page=100&page={}",
            username, page
        );

        let resp = match client
            .get(&url)
            .header(USER_AGENT, "rust-scraper")
            .header(AUTHORIZATION, format!("token {}", github_token))
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => break,
        };
        check_api_request_limit(&resp).await;

        let repos: Vec<serde_json::Value> = match resp.json().await {
            Ok(json) => json,
            Err(_) => break,
        };

        if repos.is_empty() {
            break;
        }

        for repo in repos {
            if let (Some(name), Some(html_url)) = (
                repo.get("name").and_then(|n| n.as_str()),
                repo.get("html_url").and_then(|u| u.as_str()),
            ) {
                repo_urls.push(html_url.to_string());
            }
        }

        page += 1;
    }

    let total = repo_urls.len();
    (repo_urls, total)
}

pub async fn handle_github_repo_url(
    client: &Client,
    repo_url: &str,
    github_token: &str,
    keywords: &[&str; 8],
    allowed_extensions: &[&str; 4],
    limit: usize,
    origin: &str,
) -> Result<(GitHubUpdateData, Option<String>)> {
    if let Some((owner, repo)) = parse_github_url(repo_url) {
        let (commit_sha, commit_date, email, name) =
            match get_last_commit_info(client, &owner, &repo, github_token).await {
                Some(info) => info,
                None => (
                    "".to_string(),
                    chrono::Utc::now().to_rfc3339(),
                    "".to_string(),
                    "".to_string(),
                ),
            };

        match process_repo(
            &client,
            &owner,
            &repo,
            &github_token,
            &keywords,
            &allowed_extensions,
            limit,
        )
        .await
        {
            Some((repo_map, file_types, files_processed)) => {
                let formatted_summary = format_for_mapping(
                    &owner,
                    &repo,
                    &commit_sha,
                    &commit_date,
                    &repo_map,
                    &email,
                    &name,
                    Some(origin),
                    &file_types,
                    &files_processed,
                );
                Ok((formatted_summary, None))
            }
            None => Ok((
                GitHubUpdateData::default(),
                Some("Failed to process repo data".to_string()),
            )),
        }
    } else {
        Ok((
            GitHubUpdateData::default(),
            Some("Invalid GitHub URL".to_string()),
        ))
    }
}

pub async fn search_code(
    query: &str,
    token: &str,
) -> Result<Vec<GitHubCodeItem>, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.github.com/search/code?q={}&per_page=1000",
        query
    );
    let client = Client::new();

    let res: GitHubSearchResponse = client
        .get(&url)
        .header("User-Agent", "rust-script")
        .bearer_auth(token)
        .send()
        .await?
        .json()
        .await?;

    Ok(res.items)
}

pub async fn search_github_repos(queries: [&str; 2], github_token: &str) -> Result<Vec<String>> {
    let mut seen_repos: HashSet<String> = HashSet::new();
    for query in queries {
        match search_code(query, &github_token).await {
            Ok(items) => {
                for item in items {
                    if let Some(repo_url) = get_github_repo(&item.html_url) {
                        if seen_repos.contains(&repo_url) {
                            continue;
                        }
                        seen_repos.insert(repo_url.clone());
                    }
                }
            }
            Err(e) => {
                eprintln!("Error fetching results for '{}': {}", query, e);
            }
        }
    }

    // Filter repos
    let exclude_keywords = vec!["magicblock-labs"];
    let filtered_repo_urls: Vec<String> = seen_repos
        .into_iter()
        .filter(|repo_url| {
            let url_lower = repo_url.to_lowercase();
            !exclude_keywords
                .iter()
                .any(|kw| url_lower.contains(&kw.to_lowercase()))
        })
        .collect();
    println!(
        "Found {} unique repos from queries",
        filtered_repo_urls.len()
    );
    return Ok(filtered_repo_urls);
}
