use serde::{Deserialize, Serialize};
use serde_json::{from_value, to_value, Value};
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone, Deserialize, Default, PartialEq)]
pub struct KeywordResult {
    pub count: usize,
    pub files: Vec<String>,
}
pub type RepoMap = HashMap<String, KeywordResult>;

#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct GitHubUpdateData {
    pub commit_sha: String,
    pub email: String,
    pub keyword_counts: HashMap<String, KeywordResult>,
    pub keyword_matches: String,
    pub commit_date: String,
    pub name: String,
    pub owner: String,
    pub repo_name: String,
    pub snapshot_url: String,
    pub origin: String,
    pub file_types: String,
    pub files_processed: String,

    // Optional fields:
    pub location: Option<String>,
    pub presentation_link: Option<String>,
    pub technical_link: Option<String>,
    pub tracks: Option<String>,
    pub contact: Option<String>,
    pub website_link: Option<String>,
    pub social_link: Option<String>,
    pub wallet: Option<String>,
}

impl GitHubUpdateData {
    pub fn is_empty(&self) -> bool {
        let json = serde_json::to_value(self).unwrap_or(Value::Null);
        match json {
            Value::Object(map) => map.values().all(|v| match v {
                Value::String(s) => s.is_empty(),
                Value::Null => true,
                Value::Object(m) => m.is_empty(),
                Value::Array(a) => a.is_empty(),
                _ => false,
            }),
            _ => true,
        }
    }

    pub fn add_fields_if_exist(
        &mut self,
        columns: &HashMap<String, Vec<String>>,
        fields: &[&str],
        row_index: usize,
    ) {
        let mut val = to_value(&*self).unwrap();

        if let Value::Object(ref mut map) = val {
            for &field in fields {
                let needs_update = match map.get(field) {
                    Some(Value::String(s)) => s.is_empty(),
                    Some(Value::Null) => true,
                    None => true,
                    _ => false,
                };

                if !needs_update {
                    continue;
                }

                if let Some(values) = columns.get(field) {
                    if let Some(value) = values.get(row_index) {
                        map.insert(field.to_string(), Value::String(value.clone()));
                    }
                }
            }
        }

        let new_self: GitHubUpdateData = from_value(val).unwrap();
        *self = new_self;
    }
}
