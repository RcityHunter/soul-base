#![allow(dead_code)]

pub mod config;

#[cfg(feature = "surreal")]
pub mod binder;
#[cfg(feature = "surreal")]
pub mod datastore;
#[cfg(feature = "surreal")]
pub mod errors;
#[cfg(feature = "surreal")]
pub mod mapper;
#[cfg(feature = "surreal")]
pub mod migrate;
#[cfg(feature = "surreal")]
pub mod observe;
#[cfg(feature = "surreal")]
pub mod repo;
#[cfg(feature = "surreal")]
pub mod schema;
#[cfg(feature = "surreal")]
pub mod session;
#[cfg(feature = "surreal")]
pub mod tx;

// SurrealDB 原生功能集成模块
#[cfg(feature = "surreal")]
pub mod realtime;
#[cfg(feature = "surreal")]
pub mod spatial;
#[cfg(feature = "surreal")]
pub mod timeseries;
#[cfg(feature = "surreal")]
pub mod vector;

pub use config::SurrealConfig;

#[cfg(feature = "surreal")]
pub use datastore::SurrealDatastore;
#[cfg(feature = "surreal")]
pub use mapper::SurrealMapper;
#[cfg(feature = "surreal")]
pub use migrate::SurrealMigrator;
#[cfg(feature = "surreal")]
pub use repo::{
    AwarenessEventRepo, CausalEdgeRepo, RecallChunkRepo, SurrealRepository, TimelineEventRepo,
    VectorManifestRepo,
};
#[cfg(feature = "surreal")]
pub use schema::{
    core_migration, llm_migration, migrations as schema_migrations, TABLE_AWARENESS_EVENT,
    TABLE_CAUSAL_EDGE, TABLE_LLM_EXPLAIN, TABLE_LLM_TOOL_PLAN, TABLE_RECALL_CHUNK,
    TABLE_TIMELINE_EVENT, TABLE_VECTOR_MANIFEST,
};
#[cfg(feature = "surreal")]
pub use session::SurrealSession;
#[cfg(feature = "surreal")]
pub use tx::SurrealTransaction;

// SurrealDB 原生功能导出
#[cfg(feature = "surreal")]
pub use realtime::{
    ConditionOperator, EventStream, LiveAction, LiveNotification, LiveQueryFilter,
    NotificationPattern, PatternAlert, PatternCondition, RealtimeNotifier, RealtimeStats,
    SubscriptionBuilder, SubscriptionConfig, SubscriptionHandle, SubscriptionId,
    SurrealRealtimeNotifier,
};
#[cfg(feature = "surreal")]
pub use spatial::{
    geohash, GeoBoundingBox, GeoPoint, GeoPolygon, SpatialQueryConfig, SpatialQueryExecutor,
    SpatialResult, SurrealSpatialQueryExecutor,
};
#[cfg(feature = "surreal")]
pub use timeseries::{
    AggregateFunction, PatternMatch, PatternType, PeriodComparison, SurrealTimeSeriesExecutor,
    TimeBucket, TimeGranularity, TimeSeriesExecutor, TimeSeriesQuery, TimeSeriesResult,
    TimeSeriesSummary, TimeWindow, TrendDirection, TrendResult,
};
#[cfg(feature = "surreal")]
pub use vector::{
    BatchEmbedding, BatchStoreResult, DistanceMetric, EmbeddingModel, MockEmbeddingModel,
    SurrealVectorStore, VectorIndexConfig, VectorSearchConfig, VectorSearchResult, VectorStore,
    DEFAULT_EMBEDDING_DIMENSION,
};

#[cfg(not(feature = "surreal"))]
mod stub;
#[cfg(not(feature = "surreal"))]
pub use stub::*;
