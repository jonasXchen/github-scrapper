mod elk;
mod github;
mod helper;
mod sheets;
mod types;

use anyhow::Result;
use dotenvy::dotenv;
use elk::{es_document_exists, ingest_via_logstash};
use github::{
    classify_github_url, fetch_user_repos, get_github_repo, handle_github_repo_url,
    parse_github_url, search_code, GitHubUrlType,
};
use reqwest::Client;
use sheets::{
    clean_column_names, init_sheets, read_columns_from_sheet, read_from_sheet, write_row,
    write_to_cell,
};
use std::{collections::HashSet, env, fs::File, io::Write, vec};
use types::{Config, GitHubUpdateData};

#[tokio::main]

async fn main() -> Result<()> {
    dotenv().expect("Failed to load .env file");

    let github_token = env::var("PRIVATE_GITHUB_TOKEN")?;

    let config = Config {
        spreadsheet_id: "1aYacUptAwX2bqbvy9uZFdzcVjdTB7RXmjqLo851NTxs".to_string(),
        read_sheet_name: "Headhunting".to_string(),
        write_sheet_name: "Headhunting".to_string(),
        read_range: "A:C".to_string(),
        user_col: "A".to_string(),
        update_data_col: "D".to_string(),
        search_update_data_col: "A".to_string(),
        search_write_sheet_name: "Search".to_string(),
    };

    const ALLOWED_EXTENSIONS: [&str; 4] = [".toml", ".json", ".rs", ".ts"];
    const KEYWORDS: [&str; 8] = [
        "ephemeral-rollups-sdk",
        "#[ephemeral]",
        "#[commit]",
        "#[delegate]",
        "delegate_account",
        "undelegate_account",
        "commit_accounts",
        "commit_and_undelegate_accounts",
    ];

    let sheets = init_sheets().await?;

    // let repos = read_from_sheet(&sheets, &config.spreadsheet_id, &config.read_sheet_name, &config.read_range).await?;
    let columns = read_columns_from_sheet(
        &sheets,
        &config.spreadsheet_id,
        &config.read_sheet_name,
        &config.read_range,
    )
    .await?;

    let fields = vec![
        "snapshot_url",
        "presentation_link",
        "technical_link",
        "files_processed",
        "location",
        "tracks",
        "contact",
    ];

    let cleaned_columns = clean_column_names(
        columns,
        &[
            (vec!["gh", "github", "repo"], "snapshot_url"),
            (vec!["presentation"], "presentation_link"),
            (vec!["website"], "website_link"),
            (vec!["technical", "demo"], "technical_link"),
            (vec!["files_processed"], "files_processed"),
            (vec!["location", "country"], "location"),
            (vec!["track"], "tracks"),
            (vec!["contact", "team", "twitter"], "contact"),
            (vec!["wallet", "solana"], "wallet"),
            (vec!["twitter", "social link"], "social_link"),
        ],
    );
    let repos = cleaned_columns
        .get("snapshot_url")
        .cloned()
        .unwrap_or_default();

    let client = Client::new();
    let mut final_results: Vec<GitHubUpdateData> = Vec::new();

    // Get unique repos based on queries
    let queries = [
        "\"ephemeral-rollups-sdk\" in:file filename:package.json",
        "\"ephemeral-rollups-sdk\" in:file filename:Cargo.toml",
    ];
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

    // Add repo to sheets
    let mut search_row_idx = 2;
    for repo_url in &filtered_repo_urls {
        println!("Processing {} ...", repo_url);

        let (mut update_data, error_message) = handle_github_repo_url(
            &client,
            repo_url,
            &github_token,
            &KEYWORDS,
            &ALLOWED_EXTENSIONS,
            100,
            &config.read_sheet_name,
        )
        .await?;

        // Ingest data into elasticsearch
        let es_index = env::var("ES_INDEX")?;
        let doc_id = &update_data.commit_sha;
        let document_exist = es_document_exists(&es_index, doc_id).await?;
        if !document_exist {
            // Only ingest if it's not empty/default
            if !update_data.is_empty() {
                update_data.add_fields_if_exist(&cleaned_columns, &fields, search_row_idx);
                let response = ingest_via_logstash(
                    "https://es.metacamp.sg/logstash/",
                    "ELK",
                    &serde_json::to_value(&update_data)?,
                )
                .await?;

                println!("Ingest response: {}", response);
            }
            final_results.push(update_data.clone());
        }

        println!("Writing {} row", search_row_idx);
        write_row(
            &sheets,
            &config.spreadsheet_id,
            &config.search_write_sheet_name,
            &config.search_update_data_col,
            search_row_idx,
            vec![
                repo_url.clone(),
                serde_json::to_string(&update_data)?,
                update_data.keyword_matches.to_string(),
                update_data.snapshot_url,
            ],
        )
        .await?;

        println!(
            "✅ Row {} updated in {}",
            search_row_idx, &config.search_write_sheet_name,
        );

        search_row_idx += 1;
    }

    let mut row_idx = 2;
    let row_skip = 2;
    let mut row_reading = row_idx + row_skip;
    for (idx, repo_url) in repos.iter().enumerate().skip(row_skip) {
        println!(
            "Reading row {} in {}: {}",
            row_reading, config.read_sheet_name, repo_url
        );
        match classify_github_url(&repo_url) {
            GitHubUrlType::User(owner) => {
                // Could be a user or an organization
                println!("👤 Detected GitHub user/org: {}", owner);
                let (repos, total) = fetch_user_repos(&client, &owner, &github_token).await;
                println!("🔍 Found {} repos for {}", total, owner);
                for repo_url in repos {
                    let (mut update_data, error_message) = handle_github_repo_url(
                        &client,
                        &repo_url,
                        &github_token,
                        &KEYWORDS,
                        &ALLOWED_EXTENSIONS,
                        100,
                        &config.read_sheet_name,
                    )
                    .await?;

                    // Only ingest if it's not empty/default
                    if !update_data.is_empty() {
                        update_data.add_fields_if_exist(&cleaned_columns, &fields, row_idx);
                        let response = ingest_via_logstash(
                            "https://es.metacamp.sg/logstash/",
                            "ELK",
                            &serde_json::to_value(&update_data)?,
                        )
                        .await?;

                        println!("Ingest response: {}", response);
                    }
                    final_results.push(update_data.clone());

                    // Write the update data to Sheets
                    write_row(
                        &sheets,
                        &config.spreadsheet_id,
                        &config.write_sheet_name,
                        &config.user_col,
                        row_idx,
                        vec![owner.clone(), config.read_sheet_name.clone()],
                    )
                    .await?;
                    if let Some(error) = error_message {
                        println!("❌ Error processing {}: {}", repo_url, error);
                        // Write error to config.update_data_col
                        write_to_cell(
                            &sheets,
                            &config.spreadsheet_id,
                            &config.write_sheet_name,
                            &config.update_data_col,
                            row_idx,
                            &format!("❌ Error: {}", error),
                        )
                        .await?;
                    } else {
                        write_row(
                            &sheets,
                            &config.spreadsheet_id,
                            &config.write_sheet_name,
                            &config.update_data_col,
                            row_idx,
                            vec![
                                serde_json::to_string(&update_data)?,
                                update_data.keyword_matches.to_string(),
                                update_data.snapshot_url,
                            ],
                        )
                        .await?;
                        println!("✅ Row {} updated", row_idx);
                    }
                    row_idx += 1;
                }
            }

            GitHubUrlType::Repo { owner, repo_name } => {
                println!("📦 Detected GitHub repo: {}/{}", owner, repo_name);
                let repo_url = format!("https://github.com/{}/{}", owner, repo_name);
                let (mut update_data, error_message) = handle_github_repo_url(
                    &client,
                    &repo_url,
                    &github_token,
                    &KEYWORDS,
                    &ALLOWED_EXTENSIONS,
                    100,
                    &config.read_sheet_name,
                )
                .await?;

                // Only ingest if it's not empty/default
                if !update_data.is_empty() {
                    update_data.add_fields_if_exist(&cleaned_columns, &fields, row_idx);
                    let response = ingest_via_logstash(
                        "https://es.metacamp.sg/logstash/",
                        "ELK",
                        &serde_json::to_value(&update_data)?,
                    )
                    .await?;

                    println!("Ingest response: {}", response);
                }

                final_results.push(update_data.clone());

                // Write the update data to Sheets
                if let Some(error) = error_message {
                    println!("❌ Error processing {}: {}", repo_url, error);
                    // Write error to config.update_data_col
                    write_to_cell(
                        &sheets,
                        &config.spreadsheet_id,
                        &config.write_sheet_name,
                        &config.update_data_col,
                        row_idx + row_skip,
                        &format!("❌ Error: {}", error),
                    )
                    .await?;
                } else {
                    write_row(
                        &sheets,
                        &config.spreadsheet_id,
                        &config.write_sheet_name,
                        &config.update_data_col,
                        row_idx + row_skip,
                        vec![
                            serde_json::to_string(&update_data)?,
                            update_data.keyword_matches.to_string(),
                            update_data.snapshot_url,
                        ],
                    )
                    .await?;

                    println!("✅ Row {} updated", idx + 2);
                }
                row_idx += 1;
            }

            GitHubUrlType::Invalid => {
                println!("❗ Invalid GitHub URL: {}", repo_url);
                row_idx += 1;
            }
        }
        row_reading += 1;
    }

    // Save all results
    let json = serde_json::to_string_pretty(&final_results)?;
    File::create("results.json")?.write_all(json.as_bytes())?;

    println!("✅ All results saved.");

    Ok(())
}
