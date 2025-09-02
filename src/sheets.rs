use anyhow::Result;
use google_sheets4::{api::ValueRange, Sheets};
use hyper_rustls::HttpsConnectorBuilder;
use std::collections::HashMap;
use std::env;
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};

pub async fn init_sheets() -> Result<Sheets> {
    let creds = read_service_account_key("./service-account.json").await?;

    let auth = ServiceAccountAuthenticator::builder(creds).build().await?;
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .https_or_http()
        .enable_http1()
        .build();

    let client = hyper::Client::builder().build::<_, hyper::Body>(https);
    Ok(Sheets::new(client, auth))
}

pub async fn read_from_sheet(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    range: &str,
) -> Result<Vec<String>> {
    let read_range = format!("'{}'!{}", sheet_name, range);

    let resp = sheets
        .spreadsheets()
        .values_get(spreadsheet_id, &read_range)
        .doit()
        .await?;
    let rows = resp.1.values.unwrap_or_default();
    Ok(rows
        .iter()
        .map(|r| r.get(0).unwrap_or(&"".to_string()).clone())
        .collect())
}

pub async fn read_columns_from_sheet(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    range: &str,
) -> Result<HashMap<String, Vec<String>>, anyhow::Error> {
    let read_range = format!("'{}'!{}", sheet_name, range);

    let resp = sheets
        .spreadsheets()
        .values_get(spreadsheet_id, &read_range)
        .doit()
        .await?;

    let rows = resp.1.values.unwrap_or_default();

    if rows.is_empty() {
        return Ok(HashMap::new());
    }

    let headers = &rows[0];
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    for row in rows.iter().skip(1) {
        for (i, header) in headers.iter().enumerate() {
            let value = row.get(i).cloned().unwrap_or_default();
            columns.entry(header.clone()).or_default().push(value);
        }
    }

    Ok(columns)
}

pub fn clean_column_names(
    original_columns: HashMap<String, Vec<String>>,
    rules: &[(Vec<&str>, &str)],
) -> HashMap<String, Vec<String>> {
    let mut cleaned: HashMap<String, Vec<String>> = HashMap::new();

    for (original_name, values) in original_columns {
        let renamed = rename_column(&original_name, rules);
        cleaned.entry(renamed).or_default().extend(values);
    }

    cleaned
}

pub fn rename_column(column_name: &str, rules: &[(Vec<&str>, &str)]) -> String {
    let lower = column_name.to_lowercase();

    for (keywords, new_name) in rules {
        if keywords.iter().any(|k| lower.contains(&k.to_lowercase())) {
            return new_name.to_string();
        }
    }

    column_name.to_string()
}

pub async fn write_to_cell(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    column: &str,
    row: usize,
    value: &str,
) -> Result<()> {
    let range = format!("'{}'!{}{}", sheet_name, column, row);

    let body = ValueRange {
        range: Some(range.clone()),
        values: Some(vec![vec![value.to_string()]]),
        major_dimension: Some("ROWS".to_string()),
        ..Default::default()
    };

    sheets
        .spreadsheets()
        .values_update(body, spreadsheet_id, &range)
        .value_input_option("RAW")
        .doit()
        .await?;

    Ok(())
}

pub async fn write_row(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    start_column: &str, // e.g., "A"
    row: usize,
    values: Vec<String>, // e.g., vec!["foo", "bar", "baz"]
) -> Result<()> {
    // Convert column letter (e.g., "A") to number
    let start_col_num = column_letter_to_number(start_column);
    let end_col_num = start_col_num + values.len() - 1;
    let end_column = column_number_to_letter(end_col_num);

    let range = format!(
        "'{}'!{}{}:{}{}",
        sheet_name, start_column, row, end_column, row
    );

    let body = ValueRange {
        range: Some(range.clone()),
        values: Some(vec![values]), // Single row of values
        major_dimension: Some("ROWS".to_string()),
        ..Default::default()
    };

    sheets
        .spreadsheets()
        .values_update(body, spreadsheet_id, &range)
        .value_input_option("RAW")
        .doit()
        .await?;

    Ok(())
}

fn column_letter_to_number(letter: &str) -> usize {
    letter.chars().fold(0, |acc, c| {
        acc * 26 + (c.to_ascii_uppercase() as usize - 'A' as usize + 1)
    })
}

fn column_number_to_letter(mut num: usize) -> String {
    let mut result = String::new();
    while num > 0 {
        let rem = (num - 1) % 26;
        result.insert(0, (b'A' + rem as u8) as char);
        num = (num - 1) / 26;
    }
    result
}
