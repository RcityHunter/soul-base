//! 时序指标模型

use serde::{Deserialize, Serialize};
use soulbase_types::prelude::TenantId;

use crate::Entity;

/// 时序指标 Entity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeseriesMetric {
    #[serde(skip_deserializing)]
    pub id: String,
    pub tenant: TenantId,
    pub metric_id: String,
    pub name: String,
    pub value: f64,
    pub timestamp_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ac_id: Option<String>,
}

impl Entity for TimeseriesMetric {
    const TABLE: &'static str = "timeseries_metric";

    fn id(&self) -> &str {
        &self.id
    }

    fn tenant(&self) -> &TenantId {
        &self.tenant
    }
}

/// 时序聚合查询参数
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TimeseriesAggregateQuery {
    pub metric_name: String,
    pub aggregation: String,
    pub granularity_ms: i64,
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    pub group_by: Option<String>,
}

/// 时序聚合桶
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeseriesBucket {
    pub bucket: i64,
    pub value: f64,
    pub count: Option<u64>,
}

/// 时序摘要统计
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimeseriesSummary {
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub sum: f64,
    pub count: u64,
    pub stddev: Option<f64>,
}
