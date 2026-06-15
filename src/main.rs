use anyhow::Result;
use dotenvy::dotenv;
use integration_validation::elk::{es_document_exists, ingest_via_logstash};
use integration_validation::github::{
    classify_github_url, fetch_user_repos, handle_github_repo_url, search_github_repos,
    GitHubUrlType,
};
use integration_validation::sheets::{
    clean_column_names, column_letter_to_number, column_number_to_letter, init_sheets,
    read_columns_from_sheet, resolve_or_append_columns, write_named_cells, write_row,
    write_to_cell,
};
use integration_validation::types::{Config, GitHubUpdateData};
use reqwest::Client;
use std::{collections::HashMap, env, fs::File, io::Write, vec};

fn env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|v| match v.trim().to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        })
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn row_has_value(
    columns: &HashMap<String, Vec<String>>,
    column_name: &str,
    row_index: usize,
) -> bool {
    columns
        .get(column_name)
        .and_then(|values| values.get(row_index))
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn scraper_modes() -> Result<(bool, bool)> {
    let mut run_search = env_bool("RUN_SEARCH").unwrap_or(false);
    let mut run_sheets = env_bool("RUN_SHEETS").unwrap_or(true);

    if let Ok(mode) = env::var("SCRAPER_MODE") {
        match mode.trim().to_lowercase().as_str() {
            "search" | "search-only" | "github-search" => {
                run_search = true;
                run_sheets = false;
            }
            "sheet" | "sheets" | "sheet-only" => {
                run_search = false;
                run_sheets = true;
            }
            "all" | "both" => {
                run_search = true;
                run_sheets = true;
            }
            "" => {}
            other => {
                anyhow::bail!(
                    "Invalid SCRAPER_MODE '{}'. Use search, sheet, or all.",
                    other
                );
            }
        }
    }

    Ok((run_search, run_sheets))
}

#[tokio::main]

async fn main() -> Result<()> {
    dotenv().ok();

    let (run_search, run_sheets) = scraper_modes()?;
    println!("Scraper mode: search={}, sheets={}", run_search, run_sheets);

    let github_token = env::var("PRIVATE_GITHUB_TOKEN")?;
    let default_sheet_name = env_nonempty("SCRAPER_SHEET_NAME")
        .or_else(|| env_nonempty("SHEET_NAME"))
        .unwrap_or_else(|| "Frontier".to_string());
    let continue_from_results = env_bool("SCRAPER_CONTINUE").unwrap_or(false);

    let config = Config {
        spreadsheet_id: env_nonempty("SPREADSHEET_ID")
            .unwrap_or_else(|| "1aYacUptAwX2bqbvy9uZFdzcVjdTB7RXmjqLo851NTxs".to_string()),

        // read_sheet_name: "Cypherpunk".to_string(),
        // write_sheet_name: "Cypherpunk".to_string(),
        // read_range: "A:T".to_string(),
        // update_data_col: "U".to_string(),
        // user_write_sheet: "User".to_string(),
        // user_write_col: "AA".to_string(),

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
        read_sheet_name: env_nonempty("SCRAPER_READ_SHEET_NAME")
            .unwrap_or_else(|| default_sheet_name.clone()),
        write_sheet_name: env_nonempty("SCRAPER_WRITE_SHEET_NAME").unwrap_or(default_sheet_name),
        read_range: "".to_string(),
        update_data_col: "".to_string(),
        user_write_sheet: "User".to_string(),
        user_write_col: "A".to_string(),

        search_update_data_col: "A".to_string(),
        search_write_sheet_name: "Search".to_string(),
    };

    const ALLOWED_EXTENSIONS: [&str; 4] = [".toml", ".json", ".rs", ".ts"];
    const KEYWORDS: [&str; 11] = [
        "ephemeral-rollups-sdk",
        "ephemeral-rollups-kit",
        "ephemeral-vrf-sdk",
        "#[ephemeral]",
        "#[commit]",
        "#[delegate]",
        "delegate_account",
        "undelegate_account",
        "MagicIntentBundleBuilder",
        "payments.magicblock.app",
        "await getAuthToken",
    ];

    // Resolve write locations by header name. If the matching config field is
    // empty, the resolver finds the existing anchor header or appends the
    // block at the next empty column. If the config field is set explicitly,
    // we honor that position and (re-)write the headers there.
    let update_data_headers: Vec<String> = vec![
        "Scraper Result (JSON)".to_string(),
        "Scraper Keyword Matches".to_string(),
        "Scraper Snapshot URL".to_string(),
    ];
    let user_headers: Vec<String> = vec!["Owner".to_string(), "Source Sheet".to_string()];
    let search_headers: Vec<String> = vec![
        "Repo URL".to_string(),
        "Scraper Result (JSON)".to_string(),
        "Scraper Keyword Matches".to_string(),
        "Scraper Snapshot URL".to_string(),
    ];

    // ES fields
    let fields = vec![
        "snapshot_url",
        "presentation_link",
        "technical_link",
        "files_processed",
        "location",
        "tracks",
        "contact",
    ];

    let sheets = if run_sheets {
        let sheets = init_sheets().await?;
        println!("Initialized Google Sheets API client.");
        Some(sheets)
    } else {
        println!("Skipping Google Sheets setup/read/write.");
        None
    };

    let mut update_data_cols: Vec<String> = Vec::new();
    let mut user_write_cols: Vec<String> = Vec::new();
    let mut search_cols: Vec<String> = Vec::new();
    let mut sheet_columns: HashMap<String, Vec<String>> = HashMap::new();
    let mut cleaned_columns: HashMap<String, Vec<String>> = HashMap::new();
    let mut repos: Vec<String> = Vec::new();

    if let Some(sheets) = sheets.as_ref() {
        // For each header, the resolver returns either the column letter where
        // it already lives, or a freshly appended column. When the matching
        // config field is set explicitly, we keep the old behavior: lay out the
        // headers contiguously starting at that letter.
        update_data_cols = if config.update_data_col.trim().is_empty() {
            resolve_or_append_columns(
                sheets,
                &config.spreadsheet_id,
                &config.write_sheet_name,
                &update_data_headers,
            )
            .await?
        } else {
            write_row(
                sheets,
                &config.spreadsheet_id,
                &config.write_sheet_name,
                &config.update_data_col,
                1,
                update_data_headers.clone(),
            )
            .await?;
            let start = column_letter_to_number(&config.update_data_col);
            (0..update_data_headers.len())
                .map(|i| column_number_to_letter(start + i))
                .collect()
        };

        user_write_cols = if config.user_write_col.trim().is_empty() {
            resolve_or_append_columns(
                sheets,
                &config.spreadsheet_id,
                &config.user_write_sheet,
                &user_headers,
            )
            .await?
        } else {
            write_row(
                sheets,
                &config.spreadsheet_id,
                &config.user_write_sheet,
                &config.user_write_col,
                1,
                user_headers.clone(),
            )
            .await?;
            let start = column_letter_to_number(&config.user_write_col);
            (0..user_headers.len())
                .map(|i| column_number_to_letter(start + i))
                .collect()
        };

        if run_search {
            search_cols = if config.search_update_data_col.trim().is_empty() {
                resolve_or_append_columns(
                    sheets,
                    &config.spreadsheet_id,
                    &config.search_write_sheet_name,
                    &search_headers,
                )
                .await?
            } else {
                write_row(
                    sheets,
                    &config.spreadsheet_id,
                    &config.search_write_sheet_name,
                    &config.search_update_data_col,
                    1,
                    search_headers.clone(),
                )
                .await?;
                let start = column_letter_to_number(&config.search_update_data_col);
                (0..search_headers.len())
                    .map(|i| column_number_to_letter(start + i))
                    .collect()
            };
        }

        println!(
            "Resolved write columns — update_data: {:?}, user: {:?}, search: {:?}",
            update_data_cols, user_write_cols, search_cols
        );

        sheet_columns = read_columns_from_sheet(
            sheets,
            &config.spreadsheet_id,
            &config.read_sheet_name,
            &config.read_range,
        )
        .await?;

        // Extract sheet columns and normalize.
        cleaned_columns = clean_column_names(
            sheet_columns.clone(),
            &[
                (vec!["gh", "github", "repo"], "snapshot_url"),
                (vec!["presentation"], "presentation_link"),
                (vec!["website"], "website_link"),
                (vec!["technical", "demo"], "technical_link"),
                (vec!["files_processed"], "files_processed"),
                (vec!["location", "country", "residence"], "location"),
                (vec!["track"], "tracks"),
                (vec!["contact", "telegram", "team", "twitter"], "contact"),
                (vec!["wallet", "solana"], "wallet"),
                (vec!["twitter", "social link"], "social_link"),
            ],
        );
        repos = cleaned_columns
            .get("snapshot_url")
            .cloned()
            .unwrap_or_default();
    }

    let client = Client::new();
    let mut final_results: Vec<GitHubUpdateData> = Vec::new();

    let queries = [
        "\"ephemeral-rollups-sdk\" in:file filename:package.json",
        "\"ephemeral-rollups-kit\" in:file filename:package.json",
        "\"ephemeral-vrf-sdk\" in:file filename:package.json",
        "\"ephemeral-rollups-sdk\" in:file filename:Cargo.toml",
        "\"ephemeral-rollups-pinocchio\" in:file filename:Cargo.toml",
    ];
    let filtered_repo_urls: Vec<String> = if run_search {
        search_github_repos(&queries, &github_token).await?
    } else {
        Vec::<String>::new()
    };

    let mut search_row_idx = 2;
    if filtered_repo_urls.len() > 0 {
        println!("Processing Public Search ...");
        for repo_url in &filtered_repo_urls {
            println!("Processing {} ...", repo_url);

            let (mut update_data, _error_message) = handle_github_repo_url(
                &client,
                repo_url,
                &github_token,
                &KEYWORDS,
                &ALLOWED_EXTENSIONS,
                254,
                "Public Search",
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
                    if run_sheets {
                        update_data.add_fields_if_exist(&cleaned_columns, &fields, search_row_idx);
                    }
                    let response = ingest_via_logstash(
                        "https://elk.jonas-chen.com/logstash/",
                        "ELK",
                        &serde_json::to_value(&update_data)?,
                    )
                    .await?;

                    println!("Ingest response: {}", response);
                }
                final_results.push(update_data.clone());
            }

            if let Some(sheets) = sheets.as_ref() {
                println!("Writing {} row", search_row_idx);
                write_named_cells(
                    sheets,
                    &config.spreadsheet_id,
                    &config.search_write_sheet_name,
                    search_row_idx,
                    &[
                        (&search_cols[0], repo_url.clone()),
                        (&search_cols[1], serde_json::to_string(&update_data)?),
                        (&search_cols[2], update_data.keyword_matches.to_string()),
                        (&search_cols[3], update_data.snapshot_url.clone()),
                    ],
                )
                .await?;

                println!(
                    "✅ Row {} updated in {}",
                    search_row_idx, &config.search_write_sheet_name,
                );
            } else {
                println!("Skipping Search sheet write for {}", repo_url);
            }

            search_row_idx += 1;
        }
    }

    if let Some(sheets) = sheets.as_ref() {
        // Going through Sheets
        let row_idx = 2;
        let row_skip: usize = env::var("ROW_SKIP")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        let continue_column = env_nonempty("SCRAPER_CONTINUE_COLUMN")
            .unwrap_or_else(|| update_data_headers[0].clone());
        if row_skip > 0 {
            println!(
                "Skipping first {} sheet data row(s); starting at row {}.",
                row_skip,
                row_idx + row_skip
            );
        }
        if continue_from_results {
            println!(
                "Continue mode enabled; skipping rows with a value in '{}'.",
                continue_column
            );
        }
        for (data_row_idx, repo_url) in repos.iter().enumerate().skip(row_skip) {
            let row_reading = row_idx + data_row_idx;
            let repo_url = repo_url.trim();
            if continue_from_results
                && row_has_value(&sheet_columns, &continue_column, data_row_idx)
            {
                println!(
                    "Skipping row {} because '{}' is already filled.",
                    row_reading, continue_column
                );
                continue;
            }
            if repo_url.is_empty() {
                println!(
                    "Skipping row {} because the GitHub URL cell is empty.",
                    row_reading
                );
                continue;
            }

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
                    let mut matched_user_repos = false;
                    for repo_url in repos {
                        let (mut update_data, error_message) = handle_github_repo_url(
                            &client,
                            &repo_url,
                            &github_token,
                            &KEYWORDS,
                            &ALLOWED_EXTENSIONS,
                            254,
                            &config.read_sheet_name,
                        )
                        .await?;

                        // Skip if there are no keyword matches (only record users with keyword matches) or data is empty
                        if update_data.keyword_matches == "0" || update_data.is_empty() {
                            continue;
                        }
                        matched_user_repos = true;

                        // Only ingest if it's not empty/default
                        if !update_data.is_empty() {
                            update_data.add_fields_if_exist(
                                &cleaned_columns,
                                &fields,
                                data_row_idx,
                            );
                            let response = ingest_via_logstash(
                                "https://elk.jonas-chen.com/logstash/",
                                "ELK",
                                &serde_json::to_value(&update_data)?,
                            )
                            .await?;

                            println!("Ingest response: {}", response);
                        }
                        final_results.push(update_data.clone());

                        // Write the user identity block.
                        write_named_cells(
                            sheets,
                            &config.spreadsheet_id,
                            &config.user_write_sheet,
                            row_reading,
                            &[
                                (&user_write_cols[0], owner.clone()),
                                (&user_write_cols[1], config.read_sheet_name.clone()),
                            ],
                        )
                        .await?;

                        if let Some(error) = error_message {
                            println!("❌ Error processing {}: {}", repo_url, error);
                            // Error goes into the JSON-result column.
                            write_to_cell(
                                sheets,
                                &config.spreadsheet_id,
                                &config.write_sheet_name,
                                &update_data_cols[0],
                                row_reading,
                                &format!("❌ Error: {}", error),
                            )
                            .await?;
                        } else {
                            write_named_cells(
                                sheets,
                                &config.spreadsheet_id,
                                &config.write_sheet_name,
                                row_reading,
                                &[
                                    (&update_data_cols[0], serde_json::to_string(&update_data)?),
                                    (
                                        &update_data_cols[1],
                                        update_data.keyword_matches.to_string(),
                                    ),
                                    (&update_data_cols[2], update_data.snapshot_url.clone()),
                                ],
                            )
                            .await?;
                            println!("✅ Row {} updated", row_reading);
                        }
                    }

                    if !matched_user_repos {
                        let update_data = GitHubUpdateData {
                            owner: owner.clone(),
                            origin: config.read_sheet_name.clone(),
                            keyword_matches: "0".to_string(),
                            ..Default::default()
                        };
                        final_results.push(update_data.clone());

                        write_named_cells(
                            sheets,
                            &config.spreadsheet_id,
                            &config.user_write_sheet,
                            row_reading,
                            &[
                                (&user_write_cols[0], owner.clone()),
                                (&user_write_cols[1], config.read_sheet_name.clone()),
                            ],
                        )
                        .await?;

                        write_named_cells(
                            sheets,
                            &config.spreadsheet_id,
                            &config.write_sheet_name,
                            row_reading,
                            &[
                                (&update_data_cols[0], serde_json::to_string(&update_data)?),
                                (
                                    &update_data_cols[1],
                                    update_data.keyword_matches.to_string(),
                                ),
                                (&update_data_cols[2], update_data.snapshot_url.clone()),
                            ],
                        )
                        .await?;
                        println!(
                            "✅ Row {} marked complete; no keyword matches found for {}.",
                            row_reading, owner
                        );
                    }
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
                        254,
                        &config.read_sheet_name,
                    )
                    .await?;

                    // Only ingest if it's not empty/default
                    if !update_data.is_empty() {
                        update_data.add_fields_if_exist(&cleaned_columns, &fields, data_row_idx);
                        let response = ingest_via_logstash(
                            "https://elk.jonas-chen.com/logstash/",
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
                        // Error goes into the JSON-result column.
                        write_to_cell(
                            sheets,
                            &config.spreadsheet_id,
                            &config.write_sheet_name,
                            &update_data_cols[0],
                            row_reading,
                            &format!("❌ Error: {}", error),
                        )
                        .await?;
                    } else {
                        write_named_cells(
                            sheets,
                            &config.spreadsheet_id,
                            &config.write_sheet_name,
                            row_reading,
                            &[
                                (&update_data_cols[0], serde_json::to_string(&update_data)?),
                                (
                                    &update_data_cols[1],
                                    update_data.keyword_matches.to_string(),
                                ),
                                (&update_data_cols[2], update_data.snapshot_url.clone()),
                            ],
                        )
                        .await?;

                        println!("✅ Row {} updated", row_reading);
                    }
                }

                GitHubUrlType::Invalid => {
                    println!("❗ Invalid GitHub URL: {}", repo_url);
                    write_to_cell(
                        sheets,
                        &config.spreadsheet_id,
                        &config.write_sheet_name,
                        &update_data_cols[0],
                        row_reading,
                        &format!("❌ Invalid GitHub URL: {}", repo_url),
                    )
                    .await?;
                }
            }
        }
    } else {
        println!("Skipping sheet row processing.");
    }

    // Save all results
    let json = serde_json::to_string_pretty(&final_results)?;
    File::create("results.json")?.write_all(json.as_bytes())?;

    println!("✅ All results saved.");

    Ok(())
}
