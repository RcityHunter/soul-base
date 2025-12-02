//! Time Series Query Optimization for SurrealDB
//!
//! This module provides optimized time-series queries leveraging SurrealDB's
//! native temporal capabilities for efficient event timeline analysis,
//! time-window aggregations, and trend detection.

use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;

/// Time granularity for aggregations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeGranularity {
    /// Per second
    Second,
    /// Per minute
    Minute,
    /// Per hour
    Hour,
    /// Per day
    Day,
    /// Per week
    Week,
    /// Per month
    Month,
}

impl TimeGranularity {
    /// Convert to milliseconds
    pub fn to_millis(&self) -> i64 {
        match self {
            Self::Second => 1_000,
            Self::Minute => 60_000,
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
            Self::Week => 604_800_000,
            Self::Month => 2_592_000_000, // 30 days approximation
        }
    }

    /// Get time bucket expression for SurrealQL
    pub fn bucket_expression(&self, field: &str) -> String {
        let divisor = self.to_millis();
        format!("math::floor({} / {}) * {}", field, divisor, divisor)
    }
}

/// Aggregation function
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Stddev,
    Variance,
}

impl AggregateFunction {
    /// Get SurrealQL function name
    pub fn as_surreal_fn(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "math::sum",
            Self::Avg => "math::mean",
            Self::Min => "math::min",
            Self::Max => "math::max",
            Self::First => "array::first",
            Self::Last => "array::last",
            Self::Stddev => "math::stddev",
            Self::Variance => "math::variance",
        }
    }
}

/// Time window specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeWindow {
    /// Start timestamp (milliseconds)
    pub start_ms: i64,
    /// End timestamp (milliseconds)
    pub end_ms: i64,
    /// Granularity for bucketing
    pub granularity: TimeGranularity,
}

impl TimeWindow {
    /// Create a time window from now going back N duration
    pub fn last_duration(duration_ms: i64, granularity: TimeGranularity) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            start_ms: now - duration_ms,
            end_ms: now,
            granularity,
        }
    }

    /// Create a time window for the last N hours
    pub fn last_hours(hours: u32, granularity: TimeGranularity) -> Self {
        Self::last_duration(hours as i64 * 3_600_000, granularity)
    }

    /// Create a time window for the last N days
    pub fn last_days(days: u32, granularity: TimeGranularity) -> Self {
        Self::last_duration(days as i64 * 86_400_000, granularity)
    }

    /// Number of buckets in this window
    pub fn bucket_count(&self) -> usize {
        let duration = self.end_ms - self.start_ms;
        (duration / self.granularity.to_millis()).max(1) as usize
    }
}

/// Aggregation result for a single time bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeBucket {
    /// Bucket start timestamp (milliseconds)
    pub timestamp_ms: i64,
    /// Aggregated value
    pub value: f64,
    /// Number of records in bucket
    pub count: usize,
    /// Additional breakdown by category (if grouped)
    pub breakdown: Option<HashMap<String, f64>>,
}

/// Time series aggregation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesResult {
    /// Time window used
    pub window: TimeWindow,
    /// Buckets with aggregated values
    pub buckets: Vec<TimeBucket>,
    /// Total count across all buckets
    pub total_count: usize,
    /// Summary statistics
    pub summary: TimeSeriesSummary,
}

/// Summary statistics for time series
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesSummary {
    pub min: f64,
    pub max: f64,
    pub avg: f64,
    pub sum: f64,
    pub stddev: Option<f64>,
}

/// Trend detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendResult {
    /// Detected trend direction
    pub direction: TrendDirection,
    /// Slope of the trend line
    pub slope: f64,
    /// R-squared value (goodness of fit)
    pub r_squared: f64,
    /// Percentage change over window
    pub percent_change: f64,
    /// Trend strength (0.0 to 1.0)
    pub strength: f64,
}

/// Trend direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrendDirection {
    Increasing,
    Decreasing,
    Stable,
    Volatile,
}

/// Pattern match result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternMatch {
    /// Start timestamp of match
    pub start_ms: i64,
    /// End timestamp of match
    pub end_ms: i64,
    /// Pattern type detected
    pub pattern_type: PatternType,
    /// Confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Additional details
    pub details: Value,
}

/// Types of patterns to detect
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatternType {
    /// Sudden spike in values
    Spike,
    /// Sudden drop in values
    Drop,
    /// Periodic/cyclic pattern
    Periodic,
    /// Anomaly/outlier
    Anomaly,
    /// Plateau (stable period)
    Plateau,
    /// Gradual increase
    GradualIncrease,
    /// Gradual decrease
    GradualDecrease,
}

/// Time series query configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesQuery {
    /// Table to query
    pub table: String,
    /// Timestamp field name
    pub timestamp_field: String,
    /// Value field to aggregate (optional, default is count)
    pub value_field: Option<String>,
    /// Aggregation function
    pub aggregate: AggregateFunction,
    /// Optional group by field
    pub group_by: Option<String>,
    /// Additional filter conditions
    pub filter: Option<Value>,
    /// Journey ID filter
    pub journey_id: Option<String>,
}

impl Default for TimeSeriesQuery {
    fn default() -> Self {
        Self {
            table: "timeline_event".to_string(),
            timestamp_field: "occurred_at".to_string(),
            value_field: None,
            aggregate: AggregateFunction::Count,
            group_by: None,
            filter: None,
            journey_id: None,
        }
    }
}

/// Time series query executor trait
#[async_trait]
pub trait TimeSeriesExecutor: Send + Sync {
    /// Execute a time-windowed aggregation query
    async fn aggregate(
        &self,
        tenant: &TenantId,
        window: TimeWindow,
        query: TimeSeriesQuery,
    ) -> Result<TimeSeriesResult, StorageError>;

    /// Detect trend in time series data
    async fn detect_trend(
        &self,
        tenant: &TenantId,
        window: TimeWindow,
        query: TimeSeriesQuery,
    ) -> Result<TrendResult, StorageError>;

    /// Find patterns in time series data
    async fn find_patterns(
        &self,
        tenant: &TenantId,
        window: TimeWindow,
        query: TimeSeriesQuery,
        pattern_types: Vec<PatternType>,
    ) -> Result<Vec<PatternMatch>, StorageError>;

    /// Get recent events with timestamp ordering
    async fn get_recent(
        &self,
        tenant: &TenantId,
        table: &str,
        timestamp_field: &str,
        limit: usize,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, StorageError>;

    /// Count events in time window
    async fn count_in_window(
        &self,
        tenant: &TenantId,
        table: &str,
        timestamp_field: &str,
        window: TimeWindow,
        filter: Option<Value>,
    ) -> Result<usize, StorageError>;

    /// Compare two time periods
    async fn compare_periods(
        &self,
        tenant: &TenantId,
        window_a: TimeWindow,
        window_b: TimeWindow,
        query: TimeSeriesQuery,
    ) -> Result<PeriodComparison, StorageError>;
}

/// Result of comparing two time periods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodComparison {
    /// Summary for period A
    pub period_a: TimeSeriesSummary,
    /// Summary for period B
    pub period_b: TimeSeriesSummary,
    /// Absolute change
    pub absolute_change: f64,
    /// Percentage change
    pub percent_change: f64,
    /// Statistical significance (if applicable)
    pub significance: Option<f64>,
}

/// SurrealDB implementation of TimeSeriesExecutor
#[derive(Clone)]
pub struct SurrealTimeSeriesExecutor {
    datastore: SurrealDatastore,
}

impl SurrealTimeSeriesExecutor {
    pub fn new(datastore: &SurrealDatastore) -> Self {
        Self {
            datastore: datastore.clone(),
        }
    }

    async fn run_query(&self, statement: &str, params: Value) -> Result<Vec<Value>, StorageError> {
        let session = self.datastore.session().await?;
        let outcome = session.query(statement, params).await?;
        Ok(outcome.rows)
    }

    fn build_filter_clause(&self, tenant: &TenantId, filter: Option<&Value>) -> (String, Value) {
        let mut conditions = vec!["tenant = $tenant".to_string()];
        let mut bindings = json!({ "tenant": tenant.0 });

        if let Some(Value::Object(filter_map)) = filter {
            for (key, value) in filter_map {
                let bind_key = format!("filter_{}", key.replace('.', "_"));
                conditions.push(format!("{} = ${}", key, bind_key));
                bindings[&bind_key] = value.clone();
            }
        }

        (conditions.join(" AND "), bindings)
    }

    fn calculate_trend(&self, buckets: &[TimeBucket]) -> TrendResult {
        if buckets.len() < 2 {
            return TrendResult {
                direction: TrendDirection::Stable,
                slope: 0.0,
                r_squared: 0.0,
                percent_change: 0.0,
                strength: 0.0,
            };
        }

        // Simple linear regression
        let n = buckets.len() as f64;
        let sum_x: f64 = (0..buckets.len()).map(|i| i as f64).sum();
        let sum_y: f64 = buckets.iter().map(|b| b.value).sum();
        let sum_xy: f64 = buckets
            .iter()
            .enumerate()
            .map(|(i, b)| i as f64 * b.value)
            .sum();
        let sum_x2: f64 = (0..buckets.len()).map(|i| (i * i) as f64).sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x);

        // Calculate R-squared
        let mean_y = sum_y / n;
        let ss_tot: f64 = buckets.iter().map(|b| (b.value - mean_y).powi(2)).sum();
        let ss_res: f64 = buckets
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let predicted = mean_y + slope * (i as f64 - sum_x / n);
                (b.value - predicted).powi(2)
            })
            .sum();
        let r_squared = if ss_tot > 0.0 {
            1.0 - ss_res / ss_tot
        } else {
            0.0
        };

        // Calculate percent change
        let first_value = buckets.first().map(|b| b.value).unwrap_or(0.0);
        let last_value = buckets.last().map(|b| b.value).unwrap_or(0.0);
        let percent_change = if first_value.abs() > 0.001 {
            ((last_value - first_value) / first_value) * 100.0
        } else {
            0.0
        };

        // Determine direction and strength
        let volatility: f64 = buckets
            .windows(2)
            .map(|w| (w[1].value - w[0].value).abs())
            .sum::<f64>()
            / (n - 1.0);
        let avg_value = sum_y / n;
        let normalized_volatility = if avg_value.abs() > 0.001 {
            volatility / avg_value
        } else {
            0.0
        };

        let direction = if normalized_volatility > 0.5 {
            TrendDirection::Volatile
        } else if slope.abs() < 0.01 * avg_value.abs() {
            TrendDirection::Stable
        } else if slope > 0.0 {
            TrendDirection::Increasing
        } else {
            TrendDirection::Decreasing
        };

        let strength = r_squared.abs().min(1.0);

        TrendResult {
            direction,
            slope,
            r_squared,
            percent_change,
            strength,
        }
    }

    fn detect_patterns(
        &self,
        buckets: &[TimeBucket],
        window: &TimeWindow,
        pattern_types: &[PatternType],
    ) -> Vec<PatternMatch> {
        let mut matches = Vec::new();

        if buckets.len() < 3 {
            return matches;
        }

        let values: Vec<f64> = buckets.iter().map(|b| b.value).collect();
        let mean: f64 = values.iter().sum::<f64>() / values.len() as f64;
        let stddev: f64 = (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
            / values.len() as f64)
            .sqrt();

        for pattern_type in pattern_types {
            match pattern_type {
                PatternType::Spike => {
                    // Detect values > mean + 2*stddev
                    for (i, bucket) in buckets.iter().enumerate() {
                        if bucket.value > mean + 2.0 * stddev {
                            let confidence =
                                ((bucket.value - mean) / stddev / 3.0).min(1.0).max(0.0);
                            matches.push(PatternMatch {
                                start_ms: bucket.timestamp_ms,
                                end_ms: bucket.timestamp_ms + window.granularity.to_millis(),
                                pattern_type: PatternType::Spike,
                                confidence,
                                details: json!({
                                    "value": bucket.value,
                                    "mean": mean,
                                    "stddev": stddev,
                                    "bucket_index": i,
                                }),
                            });
                        }
                    }
                }
                PatternType::Drop => {
                    // Detect values < mean - 2*stddev
                    for (i, bucket) in buckets.iter().enumerate() {
                        if bucket.value < mean - 2.0 * stddev {
                            let confidence =
                                ((mean - bucket.value) / stddev / 3.0).min(1.0).max(0.0);
                            matches.push(PatternMatch {
                                start_ms: bucket.timestamp_ms,
                                end_ms: bucket.timestamp_ms + window.granularity.to_millis(),
                                pattern_type: PatternType::Drop,
                                confidence,
                                details: json!({
                                    "value": bucket.value,
                                    "mean": mean,
                                    "stddev": stddev,
                                    "bucket_index": i,
                                }),
                            });
                        }
                    }
                }
                PatternType::Plateau => {
                    // Detect consecutive buckets with similar values
                    let threshold = stddev * 0.5;
                    let mut plateau_start = 0;
                    let mut plateau_count = 1;

                    for i in 1..buckets.len() {
                        if (buckets[i].value - buckets[i - 1].value).abs() < threshold {
                            plateau_count += 1;
                        } else {
                            if plateau_count >= 3 {
                                matches.push(PatternMatch {
                                    start_ms: buckets[plateau_start].timestamp_ms,
                                    end_ms: buckets[i - 1].timestamp_ms
                                        + window.granularity.to_millis(),
                                    pattern_type: PatternType::Plateau,
                                    confidence: (plateau_count as f64 / buckets.len() as f64)
                                        .min(1.0),
                                    details: json!({
                                        "length": plateau_count,
                                        "avg_value": buckets[plateau_start..i].iter().map(|b| b.value).sum::<f64>() / plateau_count as f64,
                                    }),
                                });
                            }
                            plateau_start = i;
                            plateau_count = 1;
                        }
                    }
                }
                PatternType::Anomaly => {
                    // Detect outliers using IQR method
                    let mut sorted: Vec<f64> = values.clone();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

                    let q1 = sorted[sorted.len() / 4];
                    let q3 = sorted[sorted.len() * 3 / 4];
                    let iqr = q3 - q1;
                    let lower = q1 - 1.5 * iqr;
                    let upper = q3 + 1.5 * iqr;

                    for (i, bucket) in buckets.iter().enumerate() {
                        if bucket.value < lower || bucket.value > upper {
                            let deviation = if bucket.value < lower {
                                (lower - bucket.value) / iqr
                            } else {
                                (bucket.value - upper) / iqr
                            };
                            matches.push(PatternMatch {
                                start_ms: bucket.timestamp_ms,
                                end_ms: bucket.timestamp_ms + window.granularity.to_millis(),
                                pattern_type: PatternType::Anomaly,
                                confidence: (deviation / 2.0).min(1.0).max(0.0),
                                details: json!({
                                    "value": bucket.value,
                                    "lower_bound": lower,
                                    "upper_bound": upper,
                                    "bucket_index": i,
                                }),
                            });
                        }
                    }
                }
                _ => {}
            }
        }

        matches
    }
}

#[async_trait]
impl TimeSeriesExecutor for SurrealTimeSeriesExecutor {
    async fn aggregate(
        &self,
        tenant: &TenantId,
        window: TimeWindow,
        query: TimeSeriesQuery,
    ) -> Result<TimeSeriesResult, StorageError> {
        let bucket_expr = window.granularity.bucket_expression(&query.timestamp_field);
        let agg_fn = query.aggregate.as_surreal_fn();

        let value_expr = match &query.value_field {
            Some(field) => format!("{}({})", agg_fn, field),
            None => format!("{}()", agg_fn),
        };

        let (filter_clause, mut bindings) = self.build_filter_clause(tenant, query.filter.as_ref());
        bindings["start_ms"] = json!(window.start_ms);
        bindings["end_ms"] = json!(window.end_ms);

        // Add journey_id filter if specified
        let journey_filter = if let Some(ref journey) = query.journey_id {
            bindings["journey_id"] = json!(journey);
            " AND journey_id = $journey_id"
        } else {
            ""
        };

        let group_clause = query
            .group_by
            .as_ref()
            .map(|f| format!(", {}", f))
            .unwrap_or_default();

        let statement = format!(
            r#"
            SELECT
                {} AS bucket_ts,
                {} AS value,
                count() AS count
                {}
            FROM {}
            WHERE {}
              AND {} >= $start_ms
              AND {} < $end_ms
              {}
            GROUP BY bucket_ts {}
            ORDER BY bucket_ts ASC;
            "#,
            bucket_expr,
            value_expr,
            group_clause,
            query.table,
            filter_clause,
            query.timestamp_field,
            query.timestamp_field,
            journey_filter,
            if query.group_by.is_some() {
                ", ".to_string() + query.group_by.as_ref().unwrap()
            } else {
                String::new()
            }
        );

        let rows = self.run_query(&statement, bindings).await?;

        // Process results into buckets
        let mut bucket_map: HashMap<i64, TimeBucket> = HashMap::new();

        for row in rows {
            let ts = row
                .get("bucket_ts")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let value = row
                .get("value")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let count = row
                .get("count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;

            let entry = bucket_map.entry(ts).or_insert_with(|| TimeBucket {
                timestamp_ms: ts,
                value: 0.0,
                count: 0,
                breakdown: if query.group_by.is_some() {
                    Some(HashMap::new())
                } else {
                    None
                },
            });

            if query.group_by.is_some() {
                if let Some(group_value) = row
                    .get(query.group_by.as_ref().unwrap())
                    .and_then(|v| v.as_str())
                {
                    if let Some(ref mut breakdown) = entry.breakdown {
                        breakdown.insert(group_value.to_string(), value);
                    }
                }
                entry.value += value;
            } else {
                entry.value = value;
            }
            entry.count += count;
        }

        let mut buckets: Vec<TimeBucket> = bucket_map.into_values().collect();
        buckets.sort_by_key(|b| b.timestamp_ms);

        // Fill in empty buckets
        let bucket_duration = window.granularity.to_millis();
        let mut filled_buckets = Vec::new();
        let mut current_ts = window.start_ms;

        while current_ts < window.end_ms {
            if let Some(bucket) = buckets.iter().find(|b| b.timestamp_ms == current_ts) {
                filled_buckets.push(bucket.clone());
            } else {
                filled_buckets.push(TimeBucket {
                    timestamp_ms: current_ts,
                    value: 0.0,
                    count: 0,
                    breakdown: None,
                });
            }
            current_ts += bucket_duration;
        }

        // Calculate summary
        let total_count: usize = filled_buckets.iter().map(|b| b.count).sum();
        let values: Vec<f64> = filled_buckets.iter().map(|b| b.value).collect();

        let summary = if values.is_empty() {
            TimeSeriesSummary {
                min: 0.0,
                max: 0.0,
                avg: 0.0,
                sum: 0.0,
                stddev: None,
            }
        } else {
            let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let sum: f64 = values.iter().sum();
            let avg = sum / values.len() as f64;
            let variance: f64 =
                values.iter().map(|v| (v - avg).powi(2)).sum::<f64>() / values.len() as f64;
            let stddev = variance.sqrt();

            TimeSeriesSummary {
                min,
                max,
                avg,
                sum,
                stddev: Some(stddev),
            }
        };

        Ok(TimeSeriesResult {
            window,
            buckets: filled_buckets,
            total_count,
            summary,
        })
    }

    async fn detect_trend(
        &self,
        tenant: &TenantId,
        window: TimeWindow,
        query: TimeSeriesQuery,
    ) -> Result<TrendResult, StorageError> {
        let result = self.aggregate(tenant, window, query).await?;
        Ok(self.calculate_trend(&result.buckets))
    }

    async fn find_patterns(
        &self,
        tenant: &TenantId,
        window: TimeWindow,
        query: TimeSeriesQuery,
        pattern_types: Vec<PatternType>,
    ) -> Result<Vec<PatternMatch>, StorageError> {
        let result = self.aggregate(tenant, window.clone(), query).await?;
        Ok(self.detect_patterns(&result.buckets, &window, &pattern_types))
    }

    async fn get_recent(
        &self,
        tenant: &TenantId,
        table: &str,
        timestamp_field: &str,
        limit: usize,
        filter: Option<Value>,
    ) -> Result<Vec<Value>, StorageError> {
        let (filter_clause, mut bindings) = self.build_filter_clause(tenant, filter.as_ref());
        bindings["limit"] = json!(limit);

        let statement = format!(
            r#"
            SELECT *, type::string(id) AS id
            FROM {}
            WHERE {}
            ORDER BY {} DESC
            LIMIT $limit;
            "#,
            table, filter_clause, timestamp_field
        );

        self.run_query(&statement, bindings).await
    }

    async fn count_in_window(
        &self,
        tenant: &TenantId,
        table: &str,
        timestamp_field: &str,
        window: TimeWindow,
        filter: Option<Value>,
    ) -> Result<usize, StorageError> {
        let (filter_clause, mut bindings) = self.build_filter_clause(tenant, filter.as_ref());
        bindings["start_ms"] = json!(window.start_ms);
        bindings["end_ms"] = json!(window.end_ms);

        let statement = format!(
            r#"
            SELECT count() AS cnt
            FROM {}
            WHERE {}
              AND {} >= $start_ms
              AND {} < $end_ms;
            "#,
            table, filter_clause, timestamp_field, timestamp_field
        );

        let rows = self.run_query(&statement, bindings).await?;

        Ok(rows
            .first()
            .and_then(|r| r.get("cnt"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize)
    }

    async fn compare_periods(
        &self,
        tenant: &TenantId,
        window_a: TimeWindow,
        window_b: TimeWindow,
        query: TimeSeriesQuery,
    ) -> Result<PeriodComparison, StorageError> {
        let result_a = self
            .aggregate(tenant, window_a, query.clone())
            .await?;
        let result_b = self.aggregate(tenant, window_b, query).await?;

        let absolute_change = result_b.summary.avg - result_a.summary.avg;
        let percent_change = if result_a.summary.avg.abs() > 0.001 {
            (absolute_change / result_a.summary.avg) * 100.0
        } else {
            0.0
        };

        Ok(PeriodComparison {
            period_a: result_a.summary,
            period_b: result_b.summary,
            absolute_change,
            percent_change,
            significance: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_granularity_millis() {
        assert_eq!(TimeGranularity::Second.to_millis(), 1_000);
        assert_eq!(TimeGranularity::Minute.to_millis(), 60_000);
        assert_eq!(TimeGranularity::Hour.to_millis(), 3_600_000);
        assert_eq!(TimeGranularity::Day.to_millis(), 86_400_000);
    }

    #[test]
    fn test_time_window_last_hours() {
        let window = TimeWindow::last_hours(24, TimeGranularity::Hour);
        assert_eq!(window.end_ms - window.start_ms, 24 * 3_600_000);
        assert_eq!(window.bucket_count(), 24);
    }

    #[test]
    fn test_time_window_last_days() {
        let window = TimeWindow::last_days(7, TimeGranularity::Day);
        assert_eq!(window.end_ms - window.start_ms, 7 * 86_400_000);
        assert_eq!(window.bucket_count(), 7);
    }

    #[test]
    fn test_aggregate_function_surreal_fn() {
        assert_eq!(AggregateFunction::Count.as_surreal_fn(), "count");
        assert_eq!(AggregateFunction::Sum.as_surreal_fn(), "math::sum");
        assert_eq!(AggregateFunction::Avg.as_surreal_fn(), "math::mean");
        assert_eq!(AggregateFunction::Min.as_surreal_fn(), "math::min");
        assert_eq!(AggregateFunction::Max.as_surreal_fn(), "math::max");
    }

    #[test]
    fn test_bucket_expression() {
        let expr = TimeGranularity::Hour.bucket_expression("occurred_at");
        assert!(expr.contains("3600000"));
        assert!(expr.contains("math::floor"));
    }

    #[test]
    fn test_time_series_summary() {
        let summary = TimeSeriesSummary {
            min: 1.0,
            max: 100.0,
            avg: 50.0,
            sum: 500.0,
            stddev: Some(25.0),
        };

        assert_eq!(summary.min, 1.0);
        assert_eq!(summary.max, 100.0);
        assert_eq!(summary.avg, 50.0);
    }

    #[test]
    fn test_trend_result() {
        let trend = TrendResult {
            direction: TrendDirection::Increasing,
            slope: 1.5,
            r_squared: 0.95,
            percent_change: 50.0,
            strength: 0.9,
        };

        assert_eq!(trend.direction, TrendDirection::Increasing);
        assert!(trend.r_squared > 0.9);
    }

    #[test]
    fn test_time_series_query_default() {
        let query = TimeSeriesQuery::default();
        assert_eq!(query.table, "timeline_event");
        assert_eq!(query.timestamp_field, "occurred_at");
        assert_eq!(query.aggregate, AggregateFunction::Count);
        assert!(query.value_field.is_none());
    }
}
