mod elk;
mod github;
mod helper;
mod sheets;
mod types;

use anyhow::Result;
use dotenvy::dotenv;
use elk::ingest_via_logstash;
use github::{classify_github_url, fetch_user_repos, handle_github_repo_url, GitHubUrlType};
use reqwest::Client;
use sheets::{
    clean_column_names, init_sheets, read_columns_from_sheet, read_from_sheet, write_row,
    write_to_cell,
};
use std::{env, fs::File, io::Write, vec};
use types::GitHubUpdateData;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().expect("Failed to load .env file");

    let github_token = env::var("PRIVATE_GITHUB_TOKEN")?;
    let spreadsheet_id = env::var("SPREADSHEET_ID")?;
    let read_sheet_name = env::var("READ_SHEET_NAME")?;
    let write_sheet_name = env::var("WRITE_SHEET_NAME")?;
    let range = env::var("READ_RANGE")?;

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

    // let repos = read_from_sheet(&sheets, &spreadsheet_id, &read_sheet_name, &range).await?;
    let columns =
        read_columns_from_sheet(&sheets, &spreadsheet_id, &read_sheet_name, &range).await?;

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
    let mut row_idx = 2;
    let row_skip = 0;
    let mut row_reading = row_idx + row_skip;
    for (idx, repo_url) in repos.iter().enumerate().skip(row_skip) {
        println!(
            "Reading row {} in {}: {}",
            row_reading, read_sheet_name, repo_url
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
                        &read_sheet_name,
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
                    let user_col = env::var("USER_COLUMN")?;
                    let update_data_col = env::var("UPDATE_DATA_COLUMN")?;
                    write_row(
                        &sheets,
                        &spreadsheet_id,
                        &write_sheet_name,
                        &user_col,
                        row_idx,
                        vec![owner.clone(), read_sheet_name.clone()],
                    )
                    .await?;
                    if let Some(error) = error_message {
                        println!("❌ Error processing {}: {}", repo_url, error);
                        // Write error to update_data_col
                        write_to_cell(
                            &sheets,
                            &spreadsheet_id,
                            &write_sheet_name,
                            &update_data_col,
                            row_idx,
                            &format!("❌ Error: {}", error),
                        )
                        .await?;
                    } else {
                        write_row(
                            &sheets,
                            &spreadsheet_id,
                            &write_sheet_name,
                            &update_data_col,
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
                    &read_sheet_name,
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
                let update_data_col = env::var("UPDATE_DATA_COLUMN")?;
                if let Some(error) = error_message {
                    println!("❌ Error processing {}: {}", repo_url, error);
                    // Write error to update_data_col
                    write_to_cell(
                        &sheets,
                        &spreadsheet_id,
                        &write_sheet_name,
                        &update_data_col,
                        row_idx + row_skip,
                        &format!("❌ Error: {}", error),
                    )
                    .await?;
                } else {
                    write_row(
                        &sheets,
                        &spreadsheet_id,
                        &write_sheet_name,
                        &update_data_col,
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
