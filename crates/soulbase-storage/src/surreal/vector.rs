//! Vector Storage and Semantic Search for SurrealDB
//!
//! This module provides vector storage capabilities leveraging SurrealDB's native
//! HNSW (Hierarchical Navigable Small World) vector index support for efficient
//! semantic similarity search.

use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use soulbase_types::prelude::TenantId;

/// Default embedding dimension (OpenAI text-embedding-ada-002)
pub const DEFAULT_EMBEDDING_DIMENSION: usize = 1536;

/// Supported distance metrics for vector similarity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    /// Cosine similarity (default, range: -1 to 1)
    #[default]
    Cosine,
    /// Euclidean distance (L2 norm)
    Euclidean,
    /// Manhattan distance (L1 norm)
    Manhattan,
    /// Minkowski distance
    Minkowski,
    /// Chebyshev distance (L∞ norm)
    Chebyshev,
}

impl DistanceMetric {
    /// Convert to SurrealDB distance function name
    pub fn as_surreal_fn(&self) -> &'static str {
        match self {
            Self::Cosine => "vector::similarity::cosine",
            Self::Euclidean => "vector::distance::euclidean",
            Self::Manhattan => "vector::distance::manhattan",
            Self::Minkowski => "vector::distance::minkowski",
            Self::Chebyshev => "vector::distance::chebyshev",
        }
    }

    /// Whether higher score means more similar (true for cosine)
    pub fn higher_is_better(&self) -> bool {
        matches!(self, Self::Cosine)
    }
}

/// Result of a vector similarity search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    /// Record ID
    pub id: String,
    /// Similarity/distance score
    pub score: f32,
    /// Source event ID (if applicable)
    pub source_event_id: Option<String>,
    /// Content snippet or summary
    pub content: Option<String>,
    /// Additional metadata
    pub metadata: Value,
}

/// Configuration for vector search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchConfig {
    /// Number of results to return
    pub top_k: usize,
    /// Minimum similarity threshold (for cosine) or max distance (for others)
    pub threshold: Option<f32>,
    /// Distance metric to use
    pub metric: DistanceMetric,
    /// Filter by journey_id
    pub journey_id: Option<String>,
    /// Filter by label/category
    pub label: Option<String>,
    /// Include content in results
    pub include_content: bool,
    /// Include metadata in results
    pub include_metadata: bool,
}

impl Default for VectorSearchConfig {
    fn default() -> Self {
        Self {
            top_k: 10,
            threshold: None,
            metric: DistanceMetric::Cosine,
            journey_id: None,
            label: None,
            include_content: true,
            include_metadata: true,
        }
    }
}

/// Configuration for vector index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Dimension of vectors
    pub dimension: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// HNSW M parameter (max connections per node, default 16)
    pub hnsw_m: Option<u32>,
    /// HNSW ef_construction parameter (default 200)
    pub hnsw_ef_construction: Option<u32>,
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        Self {
            dimension: DEFAULT_EMBEDDING_DIMENSION,
            metric: DistanceMetric::Cosine,
            hnsw_m: Some(16),
            hnsw_ef_construction: Some(200),
        }
    }
}

/// Vector storage abstraction trait
#[async_trait]
pub trait VectorStore: Send + Sync {
    /// Store an embedding for an event
    async fn store_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
        journey_id: &str,
        embedding: Vec<f32>,
        content: Option<&str>,
        metadata: Option<Value>,
    ) -> Result<(), StorageError>;

    /// Perform semantic similarity search
    async fn semantic_search(
        &self,
        tenant: &TenantId,
        query_embedding: Vec<f32>,
        config: VectorSearchConfig,
    ) -> Result<Vec<VectorSearchResult>, StorageError>;

    /// Find similar events by event ID
    async fn find_similar(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
        config: VectorSearchConfig,
    ) -> Result<Vec<VectorSearchResult>, StorageError>;

    /// Update an existing embedding
    async fn update_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
        embedding: Vec<f32>,
    ) -> Result<(), StorageError>;

    /// Delete an embedding
    async fn delete_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
    ) -> Result<(), StorageError>;

    /// Get embedding by chunk ID
    async fn get_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
    ) -> Result<Option<Vec<f32>>, StorageError>;

    /// Batch store embeddings
    async fn batch_store_embeddings(
        &self,
        tenant: &TenantId,
        embeddings: Vec<BatchEmbedding>,
    ) -> Result<BatchStoreResult, StorageError>;
}

/// Single embedding for batch operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEmbedding {
    pub chunk_id: String,
    pub journey_id: String,
    pub embedding: Vec<f32>,
    pub content: Option<String>,
    pub label: Option<String>,
    pub metadata: Option<Value>,
}

/// Result of batch embedding storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStoreResult {
    pub success_count: usize,
    pub failed_count: usize,
    pub failed_ids: Vec<String>,
}

/// SurrealDB implementation of VectorStore
#[derive(Clone)]
pub struct SurrealVectorStore {
    datastore: SurrealDatastore,
    table: String,
}

impl SurrealVectorStore {
    /// Create a new SurrealVectorStore
    pub fn new(datastore: &SurrealDatastore) -> Self {
        Self {
            datastore: datastore.clone(),
            table: "recall_chunk".to_string(),
        }
    }

    /// Create with custom table name
    pub fn with_table(datastore: &SurrealDatastore, table: &str) -> Self {
        Self {
            datastore: datastore.clone(),
            table: table.to_string(),
        }
    }

    /// Create vector index on a field
    pub async fn create_vector_index(
        &self,
        field: &str,
        config: VectorIndexConfig,
    ) -> Result<(), StorageError> {
        let index_name = format!("idx_{}_vector", field.replace('.', "_"));

        // Build HNSW parameters
        let mut hnsw_params = format!("DIMENSION {}", config.dimension);
        if let Some(m) = config.hnsw_m {
            hnsw_params.push_str(&format!(" M {}", m));
        }
        if let Some(ef) = config.hnsw_ef_construction {
            hnsw_params.push_str(&format!(" EFC {}", ef));
        }

        let statement = format!(
            "DEFINE INDEX {} ON TABLE {} FIELDS {} SEARCH HNSW {};",
            index_name, self.table, field, hnsw_params
        );

        let session = self.datastore.session().await?;
        session.query(&statement, json!({})).await?;
        Ok(())
    }

    /// Run a raw query with bindings
    async fn run_query(
        &self,
        statement: &str,
        params: Value,
    ) -> Result<Vec<Value>, StorageError> {
        let session = self.datastore.session().await?;
        let outcome = session.query(statement, params).await?;
        Ok(outcome.rows)
    }

    fn record_key(&self, id: &str) -> String {
        let prefix = format!("{}:", self.table);
        id.strip_prefix(&prefix).unwrap_or(id).to_string()
    }
}

#[async_trait]
impl VectorStore for SurrealVectorStore {
    async fn store_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
        journey_id: &str,
        embedding: Vec<f32>,
        content: Option<&str>,
        metadata: Option<Value>,
    ) -> Result<(), StorageError> {
        let key = self.record_key(chunk_id);
        let now = chrono::Utc::now().timestamp_millis();

        let mut data = json!({
            "tenant": tenant.0,
            "chunk_id": chunk_id,
            "journey_id": journey_id,
            "embedding": embedding,
            "version": 1,
            "created_at": now,
        });

        if let Some(c) = content {
            data["content"] = Value::String(c.to_string());
        }
        if let Some(m) = metadata {
            data["metadata"] = m;
        }

        let statement = r#"
            CREATE type::thing($table, $key) CONTENT $data RETURN NONE;
        "#;

        self.run_query(
            statement,
            json!({
                "table": self.table,
                "key": key,
                "data": data,
            }),
        )
        .await?;

        Ok(())
    }

    async fn semantic_search(
        &self,
        tenant: &TenantId,
        query_embedding: Vec<f32>,
        config: VectorSearchConfig,
    ) -> Result<Vec<VectorSearchResult>, StorageError> {
        let distance_fn = config.metric.as_surreal_fn();
        let higher_is_better = config.metric.higher_is_better();

        // Build WHERE clause
        let mut conditions = vec!["tenant = $tenant".to_string()];
        if let Some(ref journey) = config.journey_id {
            conditions.push(format!("journey_id = '{}'", journey));
        }
        if let Some(ref label) = config.label {
            conditions.push(format!("label = '{}'", label));
        }

        let where_clause = conditions.join(" AND ");

        // Build SELECT fields
        let mut select_fields = vec![
            "type::string(id) AS id".to_string(),
            format!("{}(embedding, $query_vec) AS score", distance_fn),
            "source_event_id".to_string(),
        ];
        if config.include_content {
            select_fields.push("content".to_string());
        }
        if config.include_metadata {
            select_fields.push("metadata".to_string());
        }

        // Build ORDER BY
        let order_dir = if higher_is_better { "DESC" } else { "ASC" };

        // Build threshold condition
        let threshold_clause = if let Some(thresh) = config.threshold {
            if higher_is_better {
                format!("AND {}(embedding, $query_vec) >= {}", distance_fn, thresh)
            } else {
                format!("AND {}(embedding, $query_vec) <= {}", distance_fn, thresh)
            }
        } else {
            String::new()
        };

        let statement = format!(
            r#"
            SELECT {}
            FROM {}
            WHERE {} {}
            ORDER BY score {}
            LIMIT $top_k;
            "#,
            select_fields.join(", "),
            self.table,
            where_clause,
            threshold_clause,
            order_dir
        );

        let rows = self
            .run_query(
                &statement,
                json!({
                    "tenant": tenant.0,
                    "query_vec": query_embedding,
                    "top_k": config.top_k,
                }),
            )
            .await?;

        let results = rows
            .into_iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                let score = row.get("score")?.as_f64()? as f32;
                let source_event_id = row
                    .get("source_event_id")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let content = row
                    .get("content")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let metadata = row.get("metadata").cloned().unwrap_or(Value::Null);

                Some(VectorSearchResult {
                    id,
                    score,
                    source_event_id,
                    content,
                    metadata,
                })
            })
            .collect();

        Ok(results)
    }

    async fn find_similar(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
        config: VectorSearchConfig,
    ) -> Result<Vec<VectorSearchResult>, StorageError> {
        // First get the embedding for the given chunk
        let embedding = self
            .get_embedding(tenant, chunk_id)
            .await?
            .ok_or_else(|| StorageError::not_found("chunk not found"))?;

        // Then search for similar vectors, excluding the original
        let mut results = self.semantic_search(tenant, embedding, config).await?;

        // Remove the original chunk from results
        results.retain(|r| !r.id.ends_with(chunk_id));

        Ok(results)
    }

    async fn update_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
        embedding: Vec<f32>,
    ) -> Result<(), StorageError> {
        let key = self.record_key(chunk_id);

        let statement = r#"
            UPDATE type::thing($table, $key)
            SET embedding = $embedding, version = version + 1
            WHERE tenant = $tenant
            RETURN NONE;
        "#;

        self.run_query(
            statement,
            json!({
                "table": self.table,
                "key": key,
                "tenant": tenant.0,
                "embedding": embedding,
            }),
        )
        .await?;

        Ok(())
    }

    async fn delete_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
    ) -> Result<(), StorageError> {
        let key = self.record_key(chunk_id);

        let statement = r#"
            DELETE type::thing($table, $key)
            WHERE tenant = $tenant;
        "#;

        self.run_query(
            statement,
            json!({
                "table": self.table,
                "key": key,
                "tenant": tenant.0,
            }),
        )
        .await?;

        Ok(())
    }

    async fn get_embedding(
        &self,
        tenant: &TenantId,
        chunk_id: &str,
    ) -> Result<Option<Vec<f32>>, StorageError> {
        let key = self.record_key(chunk_id);

        let statement = r#"
            SELECT embedding
            FROM type::thing($table, $key)
            WHERE tenant = $tenant
            LIMIT 1;
        "#;

        let rows = self
            .run_query(
                statement,
                json!({
                    "table": self.table,
                    "key": key,
                    "tenant": tenant.0,
                }),
            )
            .await?;

        if let Some(row) = rows.into_iter().next() {
            if let Some(embedding) = row.get("embedding") {
                let vec: Vec<f32> = serde_json::from_value(embedding.clone())
                    .map_err(|e| StorageError::internal(&format!("parse embedding: {}", e)))?;
                return Ok(Some(vec));
            }
        }

        Ok(None)
    }

    async fn batch_store_embeddings(
        &self,
        tenant: &TenantId,
        embeddings: Vec<BatchEmbedding>,
    ) -> Result<BatchStoreResult, StorageError> {
        let mut success_count = 0;
        let mut failed_count = 0;
        let mut failed_ids = Vec::new();
        let now = chrono::Utc::now().timestamp_millis();

        // Use transaction for batch operation
        let session = self.datastore.session().await?;

        for batch in embeddings.chunks(100) {
            // Build batch INSERT statement
            let mut inserts = Vec::new();
            for item in batch {
                let key = self.record_key(&item.chunk_id);
                let mut data = json!({
                    "tenant": tenant.0,
                    "chunk_id": item.chunk_id,
                    "journey_id": item.journey_id,
                    "embedding": item.embedding,
                    "version": 1,
                    "created_at": now,
                });

                if let Some(ref c) = item.content {
                    data["content"] = Value::String(c.clone());
                }
                if let Some(ref l) = item.label {
                    data["label"] = Value::String(l.clone());
                }
                if let Some(ref m) = item.metadata {
                    data["metadata"] = m.clone();
                }

                inserts.push(json!({
                    "table": self.table,
                    "key": key,
                    "data": data,
                }));
            }

            // Execute batch
            for (idx, insert) in inserts.into_iter().enumerate() {
                let statement = r#"
                    CREATE type::thing($table, $key) CONTENT $data RETURN NONE;
                "#;

                match session.query(statement, insert.clone()).await {
                    Ok(_) => success_count += 1,
                    Err(_) => {
                        failed_count += 1;
                        if let Some(chunk_id) = batch.get(idx).map(|b| b.chunk_id.clone()) {
                            failed_ids.push(chunk_id);
                        }
                    }
                }
            }
        }

        Ok(BatchStoreResult {
            success_count,
            failed_count,
            failed_ids,
        })
    }
}

/// Embedding model abstraction for generating embeddings
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    /// Generate embedding for text
    async fn embed(&self, text: &str) -> Result<Vec<f32>, StorageError>;

    /// Generate embeddings for multiple texts
    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, StorageError>;

    /// Get embedding dimension
    fn dimension(&self) -> usize;

    /// Get model name/identifier
    fn model_name(&self) -> &str;
}

/// Mock embedding model for testing
#[derive(Clone)]
pub struct MockEmbeddingModel {
    dimension: usize,
}

impl MockEmbeddingModel {
    pub fn new(dimension: usize) -> Self {
        Self { dimension }
    }
}

#[async_trait]
impl EmbeddingModel for MockEmbeddingModel {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, StorageError> {
        // Generate deterministic mock embedding based on text hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let hash = hasher.finish();

        let mut embedding = Vec::with_capacity(self.dimension);
        let mut seed = hash;
        for _ in 0..self.dimension {
            // Simple LCG for reproducible random values
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let val = ((seed >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0;
            embedding.push(val);
        }

        // Normalize the vector
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut embedding {
                *v /= norm;
            }
        }

        Ok(embedding)
    }

    async fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, StorageError> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn model_name(&self) -> &str {
        "mock-embedding-model"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_metric_surreal_fn() {
        assert_eq!(
            DistanceMetric::Cosine.as_surreal_fn(),
            "vector::similarity::cosine"
        );
        assert_eq!(
            DistanceMetric::Euclidean.as_surreal_fn(),
            "vector::distance::euclidean"
        );
    }

    #[test]
    fn test_vector_search_config_default() {
        let config = VectorSearchConfig::default();
        assert_eq!(config.top_k, 10);
        assert!(config.threshold.is_none());
        assert_eq!(config.metric, DistanceMetric::Cosine);
        assert!(config.include_content);
        assert!(config.include_metadata);
    }

    #[test]
    fn test_vector_index_config_default() {
        let config = VectorIndexConfig::default();
        assert_eq!(config.dimension, DEFAULT_EMBEDDING_DIMENSION);
        assert_eq!(config.metric, DistanceMetric::Cosine);
        assert_eq!(config.hnsw_m, Some(16));
        assert_eq!(config.hnsw_ef_construction, Some(200));
    }

    #[tokio::test]
    async fn test_mock_embedding_model() {
        let model = MockEmbeddingModel::new(128);

        // Same text should produce same embedding
        let emb1 = model.embed("hello world").await.unwrap();
        let emb2 = model.embed("hello world").await.unwrap();
        assert_eq!(emb1, emb2);
        assert_eq!(emb1.len(), 128);

        // Different texts should produce different embeddings
        let emb3 = model.embed("goodbye world").await.unwrap();
        assert_ne!(emb1, emb3);

        // Embedding should be normalized
        let norm: f32 = emb1.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_mock_embedding_batch() {
        let model = MockEmbeddingModel::new(64);
        let texts = vec!["hello", "world", "test"];
        let embeddings = model.embed_batch(&texts).await.unwrap();

        assert_eq!(embeddings.len(), 3);
        for emb in &embeddings {
            assert_eq!(emb.len(), 64);
        }
    }

    #[test]
    fn test_batch_store_result() {
        let result = BatchStoreResult {
            success_count: 95,
            failed_count: 5,
            failed_ids: vec!["chunk_1".to_string(), "chunk_2".to_string()],
        };

        assert_eq!(result.success_count, 95);
        assert_eq!(result.failed_count, 5);
        assert_eq!(result.failed_ids.len(), 2);
    }
}
