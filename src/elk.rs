use anyhow::{bail, Result};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;

pub async fn ingest_to_elasticsearch<T: Serialize>(
    client: &Client,
    index: &str,
    data: Vec<T>,
    doc_id_field_name: &str,
) -> Result<()> {
    let es_url =
        std::env::var("ELASTICSEARCH_URL").unwrap_or_else(|_| "http://localhost:9200".to_string());

    for item in data {
        let doc_json = serde_json::to_value(&item)?;

        // Extract the doc_id from the specified field
        let doc_id = doc_json.get(doc_id_field_name).and_then(|v| v.as_str());

        let url = if let Some(id) = doc_id {
            format!("{}/{}/_doc/{}", es_url, index, id)
        } else {
            format!("{}/{}/_doc", es_url, index)
        };

        let res = client.post(&url).json(&doc_json).send().await?;

        if !res.status().is_success() {
            let err_body = res.text().await.unwrap_or_default();
            eprintln!("❌ Failed to ingest doc at {}: {}", url, err_body);
        } else {
            println!("✅ Ingested doc to index: {}", index);
        }
    }

    Ok(())
}

pub async fn ingest_via_logstash(
    endpoint: &str,
    api_key: &str,
    payload: &Value,
) -> Result<String, anyhow::Error> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?; // This can also error

    let res = client
        .post(endpoint)
        .header("Content-Type", "application/json")
        .header("X-Api-Key", api_key)
        .json(payload)
        .send()
        .await?;

    let status = res.status();
    let body = res
        .text()
        .await
        .unwrap_or_else(|e| format!("(Failed to read body: {})", e));

    if !status.is_success() {
        bail!("Logstash ingest failed: {} - {}", status, body);
    }

    Ok(format!("Status: {}, Body: {}", status, body))
}
