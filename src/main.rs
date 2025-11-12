mod elk;
mod github;
mod helper;
mod sheets;
mod types;

use anyhow::Result;
use dotenvy::dotenv;
use elk::{es_document_exists, ingest_via_logstash};
use github::{
    classify_github_url, fetch_user_repos, handle_github_repo_url, search_github_repos,
    GitHubUrlType,
};
use reqwest::Client;
use sheets::{clean_column_names, init_sheets, read_columns_from_sheet, write_row, write_to_cell};
use std::{collections::HashSet, env, fs::File, io::Write, vec};
use types::{Config, GitHubUpdateData};

#[tokio::main]

async fn main() -> Result<()> {
    dotenv().ok();

    let github_token = env::var("PRIVATE_GITHUB_TOKEN")?;

    let config = Config {
        spreadsheet_id: "1aYacUptAwX2bqbvy9uZFdzcVjdTB7RXmjqLo851NTxs".to_string(),

        read_sheet_name: "Cypherpunk".to_string(),
        write_sheet_name: "Cypherpunk".to_string(),
        read_range: "A:T".to_string(),
        update_data_col: "U".to_string(),
        user_write_sheet: "User".to_string(),
        user_write_col: "AA".to_string(),

        // read_sheet_name: "Cypherpunk Side Track".to_string(),
        // write_sheet_name: "Cypherpunk Side Track".to_string(),
        // read_range: "C:Z".to_string(),
        // update_data_col: "AA".to_string(),
        // user_write_sheet: "User".to_string(),
        // user_write_col: "AA".to_string(),

        // read_sheet_name: "Magic Incubator".to_string(),
        // write_sheet_name: "Magic Incubator".to_string(),
        // read_range: "A:Y".to_string(),
        // update_data_col: "AA".to_string(),
        // user_write_sheet: "User".to_string(),
        // user_write_col: "AA".to_string(),
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
    print!("Initialized Google Sheets API client.\n");

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
            (vec!["location", "country", "residence"], "location"),
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

    let queries = [
        "\"ephemeral-rollups-sdk\" in:file filename:package.json",
        "\"ephemeral-rollups-kit\" in:file filename:package.json",
        "\"ephemeral-rollups-sdk\" in:file filename:Cargo.toml",
    ];
    // let filtered_repo_urls: Vec<String> = search_github_repos(queries, &github_token).await?;
    let filtered_repo_urls = ["".to_string()].to_vec();
    // Add repos to sheets
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
        if update_data.commit_sha.is_empty() {
            println!(
                "❌ No commit SHA found for {}, skipping ingestion.",
                repo_url
            );
            continue;
        }
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

    // Going through Sheets
    let mut row_idx = 2;
    let row_skip = 1405;
    let mut row_reading = row_idx + row_skip;
    for (idx, repo_url) in repos.iter().enumerate().skip(row_skip) {
        println!(
            "Reading row {} in {}: {}",
            row_reading, config.read_sheet_name, repo_url
        );
        match classify_github_url(&repo_url) {
            // If GitHub User
            GitHubUrlType::User(owner) => {
                // Could be a user or an organization
                println!("👤 Detected GitHub user/org: {}", owner);
                let (repos, total) = fetch_user_repos(&client, &owner, &github_token, 10).await;
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

                    // Skip if there are no keyword matches (only record users with keyword matches) or data is empty
                    if (update_data.keyword_matches != "0" || update_data.is_empty()) {
                        continue;
                    }

                    // Only ingest if it's not empty/default
                    if !update_data.is_empty() {
                        update_data.add_fields_if_exist(&cleaned_columns, &fields, row_reading);
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
                        &config.user_write_col,
                        row_reading,
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
                            row_reading,
                            &format!("❌ Error: {}", error),
                        )
                        .await?;
                    } else {
                        write_row(
                            &sheets,
                            &config.spreadsheet_id,
                            &config.write_sheet_name,
                            &config.update_data_col,
                            row_reading,
                            vec![
                                serde_json::to_string(&update_data)?,
                                update_data.keyword_matches.to_string(),
                                update_data.snapshot_url,
                            ],
                        )
                        .await?;
                        println!("✅ Row {} updated", row_reading);
                    }
                }
                row_reading += 1;
            }

            // If GitHub Repo
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
                    update_data.add_fields_if_exist(&cleaned_columns, &fields, row_reading);
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
                        row_reading,
                        &format!("❌ Error: {}", error),
                    )
                    .await?;
                } else {
                    write_row(
                        &sheets,
                        &config.spreadsheet_id,
                        &config.write_sheet_name,
                        &config.update_data_col,
                        row_reading,
                        vec![
                            serde_json::to_string(&update_data)?,
                            update_data.keyword_matches.to_string(),
                            update_data.snapshot_url,
                        ],
                    )
                    .await?;

                    println!("✅ Row {} updated", idx + 2);
                }
                row_reading += 1;
            }

            GitHubUrlType::Invalid => {
                println!("❗ Invalid GitHub URL: {}", repo_url);
                row_reading += 1;
            }
        }
    }

    // Save all results
    let json = serde_json::to_string_pretty(&final_results)?;
    File::create("results.json")?.write_all(json.as_bytes())?;

    println!("✅ All results saved.");

    Ok(())
}
