use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Clone, Deserialize, Default, PartialEq)]
pub struct KeywordResult {
    pub count: usize,
    pub files: Vec<String>,
}
pub type RepoMap = HashMap<String, KeywordResult>;

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
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
}
