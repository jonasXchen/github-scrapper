use anyhow::Result;
use google_sheets4::{
    api::{
        AppendDimensionRequest, BatchUpdateSpreadsheetRequest, BatchUpdateValuesRequest, Request,
        ValueRange,
    },
    Sheets,
};
use base64::{engine::general_purpose, Engine as _};
use hyper_rustls::HttpsConnectorBuilder;
use std::collections::HashMap;
use yup_oauth2::{ServiceAccountAuthenticator, ServiceAccountKey};

pub async fn init_sheets() -> Result<Sheets> {
    let creds_b64 = std::env::var("GOOGLE_SERVICE_ACCOUNT_B64")
        .expect("Missing GOOGLE_SERVICE_ACCOUNT_B64 env var");
    let creds_json = general_purpose::STANDARD.decode(creds_b64)?;
    let creds: ServiceAccountKey = serde_json::from_slice(&creds_json)?;

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
    let read_range = if range.trim().is_empty() {
        format!("'{}'", sheet_name)
    } else {
        format!("'{}'!{}", sheet_name, range)
    };

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
    let read_range = if range.trim().is_empty() {
        format!("'{}'", sheet_name)
    } else {
        format!("'{}'!{}", sheet_name, range)
    };

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
    values: Vec<String>,
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

/// Submit many cell/range writes as a single Sheets API call.
/// `values.batchUpdate` counts as ONE write request against the
/// 60/min/user quota regardless of how many ranges it contains.
pub async fn batch_update_values(
    sheets: &Sheets,
    spreadsheet_id: &str,
    updates: Vec<ValueRange>,
) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let req = BatchUpdateValuesRequest {
        value_input_option: Some("RAW".to_string()),
        data: Some(updates),
        ..Default::default()
    };
    sheets
        .spreadsheets()
        .values_batch_update(req, spreadsheet_id)
        .doit()
        .await?;
    Ok(())
}

/// Resolves a write location by header name. If `header_name` already exists
/// in row 1, returns its column letter. Otherwise writes the header into the
/// next empty column and returns that letter.
pub async fn resolve_or_append_column(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    header_name: &str,
) -> Result<String> {
    let range = format!("'{}'!1:1", sheet_name);
    let resp = sheets
        .spreadsheets()
        .values_get(spreadsheet_id, &range)
        .doit()
        .await?;
    let headers = resp
        .1
        .values
        .unwrap_or_default()
        .into_iter()
        .next()
        .unwrap_or_default();

    if let Some(idx) = headers.iter().position(|h| h == header_name) {
        return Ok(column_number_to_letter(idx + 1));
    }

    let next_col = column_number_to_letter(headers.len() + 1);
    write_to_cell(sheets, spreadsheet_id, sheet_name, &next_col, 1, header_name).await?;
    Ok(next_col)
}

/// Make sure the named sheet has at least `needed_cols` columns. If it
/// already does, returns immediately. Otherwise appends the missing columns
/// via a single `batchUpdate` call. New sheets default to 26 columns and
/// Google Sheets enforces this as a hard limit on writes.
pub async fn ensure_grid_columns(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    needed_cols: usize,
) -> Result<()> {
    let resp = sheets.spreadsheets().get(spreadsheet_id).doit().await?;
    let spreadsheet = resp.1;
    let sheet = spreadsheet
        .sheets
        .unwrap_or_default()
        .into_iter()
        .find(|s| {
            s.properties
                .as_ref()
                .and_then(|p| p.title.as_deref())
                == Some(sheet_name)
        })
        .ok_or_else(|| anyhow::anyhow!("sheet '{}' not found in spreadsheet", sheet_name))?;
    let props = sheet
        .properties
        .ok_or_else(|| anyhow::anyhow!("sheet '{}' missing properties", sheet_name))?;
    let sheet_id = props
        .sheet_id
        .ok_or_else(|| anyhow::anyhow!("sheet '{}' missing sheet_id", sheet_name))?;
    let current_cols = props
        .grid_properties
        .and_then(|g| g.column_count)
        .unwrap_or(0) as usize;

    if needed_cols <= current_cols {
        return Ok(());
    }

    let to_add = (needed_cols - current_cols) as i32;
    let req = BatchUpdateSpreadsheetRequest {
        requests: Some(vec![Request {
            append_dimension: Some(AppendDimensionRequest {
                sheet_id: Some(sheet_id),
                dimension: Some("COLUMNS".to_string()),
                length: Some(to_add),
            }),
            ..Default::default()
        }]),
        ..Default::default()
    };
    sheets
        .spreadsheets()
        .batch_update(req, spreadsheet_id)
        .doit()
        .await?;
    Ok(())
}

/// Resolves each header in `headers` to a column letter. For each header:
///   - if a column with that exact name exists in row 1, returns its letter
///     and leaves the existing column alone (preserves prior data);
///   - otherwise appends the header at the next empty column on the right
///     and returns the new letter.
/// Missing headers are written in a single batched API call. Returned
/// letters are in the same order as the input `headers`.
pub async fn resolve_or_append_columns(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    headers: &[String],
) -> Result<Vec<String>> {
    if headers.is_empty() {
        return Ok(Vec::new());
    }
    let range = format!("'{}'!1:1", sheet_name);
    let resp = sheets
        .spreadsheets()
        .values_get(spreadsheet_id, &range)
        .doit()
        .await?;
    let mut existing: Vec<String> = resp
        .1
        .values
        .unwrap_or_default()
        .into_iter()
        .next()
        .unwrap_or_default();

    let mut letters: Vec<String> = Vec::with_capacity(headers.len());
    let mut header_writes: Vec<ValueRange> = Vec::new();
    let mut max_col_used: usize = existing.len();

    for header in headers {
        if let Some(idx) = existing.iter().position(|h| h == header) {
            letters.push(column_number_to_letter(idx + 1));
        } else {
            let col_num = existing.len() + 1;
            let col = column_number_to_letter(col_num);
            existing.push(header.clone());
            max_col_used = max_col_used.max(col_num);
            header_writes.push(ValueRange {
                range: Some(format!("'{}'!{}1", sheet_name, col)),
                values: Some(vec![vec![header.clone()]]),
                major_dimension: Some("ROWS".to_string()),
                ..Default::default()
            });
            letters.push(col);
        }
    }

    // Expand the grid first if any header would land past the current edge.
    ensure_grid_columns(sheets, spreadsheet_id, sheet_name, max_col_used).await?;
    batch_update_values(sheets, spreadsheet_id, header_writes).await?;
    Ok(letters)
}

/// Writes multiple `(column_letter, value)` pairs in a single row using one
/// batched API call. Use when the target columns aren't contiguous.
pub async fn write_named_cells(
    sheets: &Sheets,
    spreadsheet_id: &str,
    sheet_name: &str,
    row: usize,
    cells: &[(&str, String)],
) -> Result<()> {
    if cells.is_empty() {
        return Ok(());
    }
    let updates: Vec<ValueRange> = cells
        .iter()
        .map(|(col, val)| ValueRange {
            range: Some(format!("'{}'!{}{}", sheet_name, col, row)),
            values: Some(vec![vec![val.clone()]]),
            major_dimension: Some("ROWS".to_string()),
            ..Default::default()
        })
        .collect();
    batch_update_values(sheets, spreadsheet_id, updates).await
}

pub fn column_letter_to_number(letter: &str) -> usize {
    letter.chars().fold(0, |acc, c| {
        acc * 26 + (c.to_ascii_uppercase() as usize - 'A' as usize + 1)
    })
}

pub fn column_number_to_letter(mut num: usize) -> String {
    let mut result = String::new();
    while num > 0 {
        let rem = (num - 1) % 26;
        result.insert(0, (b'A' + rem as u8) as char);
        num = (num - 1) / 26;
    }
    result
}
