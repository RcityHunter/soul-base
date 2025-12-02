//! Real-time Notifications using SurrealDB LIVE QUERIES
//!
//! This module provides real-time event streaming capabilities leveraging
//! SurrealDB's native LIVE QUERY support for subscribing to data changes
//! and pattern-based notifications.

use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use soulbase_types::prelude::TenantId;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Subscription ID type
pub type SubscriptionId = String;

/// Live query action type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LiveAction {
    /// Record was created
    Create,
    /// Record was updated
    Update,
    /// Record was deleted
    Delete,
}

/// A single notification from a live query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveNotification {
    /// Subscription that generated this notification
    pub subscription_id: SubscriptionId,
    /// Action that triggered the notification
    pub action: LiveAction,
    /// The record data (after change for CREATE/UPDATE, before for DELETE)
    pub record: Value,
    /// Previous record data (for UPDATE)
    pub previous: Option<Value>,
    /// Timestamp when the change occurred
    pub timestamp_ms: i64,
    /// Table that was affected
    pub table: String,
}

/// Filter for live query subscriptions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LiveQueryFilter {
    /// Filter by tenant ID
    pub tenant_id: Option<String>,
    /// Filter by specific field values
    pub fields: HashMap<String, Value>,
    /// Filter by journey ID (common case)
    pub journey_id: Option<String>,
    /// Filter by event type (for event tables)
    pub event_type: Option<String>,
    /// Custom WHERE clause (advanced)
    pub custom_where: Option<String>,
}

impl LiveQueryFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by tenant
    pub fn with_tenant(mut self, tenant_id: &str) -> Self {
        self.tenant_id = Some(tenant_id.to_string());
        self
    }

    /// Filter by journey
    pub fn with_journey(mut self, journey_id: &str) -> Self {
        self.journey_id = Some(journey_id.to_string());
        self
    }

    /// Filter by event type
    pub fn with_event_type(mut self, event_type: &str) -> Self {
        self.event_type = Some(event_type.to_string());
        self
    }

    /// Add field filter
    pub fn with_field(mut self, field: &str, value: Value) -> Self {
        self.fields.insert(field.to_string(), value);
        self
    }

    /// Build WHERE clause for SurrealQL
    pub fn build_where_clause(&self) -> String {
        let mut conditions = Vec::new();

        if let Some(ref tenant) = self.tenant_id {
            conditions.push(format!("tenant = '{}'", tenant));
        }

        if let Some(ref journey) = self.journey_id {
            conditions.push(format!("journey_id = '{}'", journey));
        }

        if let Some(ref event_type) = self.event_type {
            conditions.push(format!("event_type = '{}'", event_type));
        }

        for (field, value) in &self.fields {
            match value {
                Value::String(s) => conditions.push(format!("{} = '{}'", field, s)),
                Value::Number(n) => conditions.push(format!("{} = {}", field, n)),
                Value::Bool(b) => conditions.push(format!("{} = {}", field, b)),
                _ => {}
            }
        }

        if let Some(ref custom) = self.custom_where {
            conditions.push(custom.clone());
        }

        if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        }
    }
}

/// Subscription configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    /// Table to subscribe to
    pub table: String,
    /// Filter for the subscription
    pub filter: LiveQueryFilter,
    /// Whether to include the full record in notifications
    pub include_record: bool,
    /// Whether to include the previous record for updates
    pub include_previous: bool,
    /// Debounce interval in milliseconds (0 = no debounce)
    pub debounce_ms: u64,
    /// Maximum notifications per second (0 = unlimited)
    pub rate_limit: u32,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            table: "timeline_event".to_string(),
            filter: LiveQueryFilter::default(),
            include_record: true,
            include_previous: false,
            debounce_ms: 0,
            rate_limit: 0,
        }
    }
}

/// Pattern definition for pattern-based notifications
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPattern {
    /// Pattern name/identifier
    pub name: String,
    /// Description of what this pattern detects
    pub description: Option<String>,
    /// Tables involved in the pattern
    pub tables: Vec<String>,
    /// Conditions that trigger the pattern
    pub conditions: Vec<PatternCondition>,
    /// Time window for temporal patterns (milliseconds)
    pub time_window_ms: Option<i64>,
    /// Minimum occurrences to trigger
    pub min_occurrences: u32,
    /// Cooldown period between triggers (milliseconds)
    pub cooldown_ms: Option<i64>,
}

/// A condition within a notification pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternCondition {
    /// Field to check
    pub field: String,
    /// Operator for comparison
    pub operator: ConditionOperator,
    /// Value to compare against
    pub value: Value,
}

/// Comparison operators for pattern conditions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOperator {
    Equals,
    NotEquals,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    Contains,
    StartsWith,
    EndsWith,
    In,
    NotIn,
    IsNull,
    IsNotNull,
}

impl ConditionOperator {
    /// Convert to SurrealQL operator
    pub fn as_surreal_op(&self) -> &'static str {
        match self {
            Self::Equals => "=",
            Self::NotEquals => "!=",
            Self::GreaterThan => ">",
            Self::LessThan => "<",
            Self::GreaterOrEqual => ">=",
            Self::LessOrEqual => "<=",
            Self::Contains => "CONTAINS",
            Self::StartsWith => "STARTS WITH",
            Self::EndsWith => "ENDS WITH",
            Self::In => "IN",
            Self::NotIn => "NOT IN",
            Self::IsNull => "IS NONE",
            Self::IsNotNull => "IS NOT NONE",
        }
    }
}

/// Pattern alert when a pattern is triggered
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternAlert {
    /// Pattern that was triggered
    pub pattern_name: String,
    /// Records that triggered the pattern
    pub matching_records: Vec<Value>,
    /// When the pattern was triggered
    pub triggered_at_ms: i64,
    /// Occurrence count (if applicable)
    pub occurrence_count: u32,
    /// Additional context
    pub context: Value,
}

/// Subscription handle for managing active subscriptions
#[derive(Debug, Clone)]
pub struct SubscriptionHandle {
    /// Unique subscription ID
    pub id: SubscriptionId,
    /// Configuration used
    pub config: SubscriptionConfig,
    /// Whether the subscription is active
    pub active: bool,
    /// When the subscription was created
    pub created_at_ms: i64,
    /// Last notification timestamp
    pub last_notification_ms: Option<i64>,
    /// Total notifications received
    pub notification_count: u64,
}

/// Callback type for notifications (sync version for in-memory use)
pub type NotificationCallback = Box<dyn Fn(LiveNotification) + Send + Sync>;

/// Callback type for pattern alerts
pub type PatternAlertCallback = Box<dyn Fn(PatternAlert) + Send + Sync>;

/// Real-time notifier trait
#[async_trait]
pub trait RealtimeNotifier: Send + Sync {
    /// Subscribe to events on a table
    async fn subscribe(
        &self,
        tenant: &TenantId,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionHandle, StorageError>;

    /// Unsubscribe from events
    async fn unsubscribe(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError>;

    /// Get active subscription by ID
    fn get_subscription(&self, subscription_id: &SubscriptionId) -> Option<SubscriptionHandle>;

    /// List all active subscriptions for a tenant
    fn list_subscriptions(&self, tenant: &TenantId) -> Vec<SubscriptionHandle>;

    /// Subscribe to a pattern
    async fn subscribe_pattern(
        &self,
        tenant: &TenantId,
        pattern: NotificationPattern,
    ) -> Result<SubscriptionId, StorageError>;

    /// Unsubscribe from a pattern
    async fn unsubscribe_pattern(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError>;

    /// Pause a subscription
    async fn pause_subscription(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError>;

    /// Resume a paused subscription
    async fn resume_subscription(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError>;

    /// Get subscription statistics
    fn get_stats(&self) -> RealtimeStats;
}

/// Statistics for real-time subscriptions
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RealtimeStats {
    /// Total active subscriptions
    pub active_subscriptions: usize,
    /// Total paused subscriptions
    pub paused_subscriptions: usize,
    /// Total notifications sent
    pub total_notifications: u64,
    /// Notifications in the last minute
    pub notifications_per_minute: u64,
    /// Total pattern subscriptions
    pub pattern_subscriptions: usize,
    /// Total pattern alerts triggered
    pub pattern_alerts_triggered: u64,
}

/// In-memory subscription state
struct SubscriptionState {
    handle: SubscriptionHandle,
    tenant_id: String,
    surreal_live_id: Option<String>,
}

/// In-memory pattern subscription state
struct PatternSubscriptionState {
    id: SubscriptionId,
    tenant_id: String,
    pattern: NotificationPattern,
    last_triggered_ms: Option<i64>,
    trigger_count: u64,
}

/// SurrealDB implementation of RealtimeNotifier
pub struct SurrealRealtimeNotifier {
    datastore: SurrealDatastore,
    subscriptions: Arc<RwLock<HashMap<SubscriptionId, SubscriptionState>>>,
    pattern_subscriptions: Arc<RwLock<HashMap<SubscriptionId, PatternSubscriptionState>>>,
    stats: Arc<RwLock<RealtimeStats>>,
}

impl SurrealRealtimeNotifier {
    /// Create a new SurrealRealtimeNotifier
    pub fn new(datastore: &SurrealDatastore) -> Self {
        Self {
            datastore: datastore.clone(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            pattern_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(RealtimeStats::default())),
        }
    }

    /// Generate a unique subscription ID
    fn generate_subscription_id() -> SubscriptionId {
        uuid::Uuid::new_v4().to_string()
    }

    /// Build LIVE SELECT statement
    fn build_live_select(&self, config: &SubscriptionConfig) -> String {
        let where_clause = config.filter.build_where_clause();

        if config.include_previous {
            format!(
                "LIVE SELECT DIFF FROM {} {}",
                config.table, where_clause
            )
        } else {
            format!(
                "LIVE SELECT * FROM {} {}",
                config.table, where_clause
            )
        }
    }

    /// Process a notification from SurrealDB
    fn process_notification(
        &self,
        subscription_id: &SubscriptionId,
        _action: LiveAction,
        _record: Value,
        _previous: Option<Value>,
    ) {
        let now = chrono::Utc::now().timestamp_millis();

        // Update subscription state
        {
            let mut subs = self.subscriptions.write();
            if let Some(state) = subs.get_mut(subscription_id) {
                state.handle.last_notification_ms = Some(now);
                state.handle.notification_count += 1;
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_notifications += 1;
        }
    }
}

#[async_trait]
impl RealtimeNotifier for SurrealRealtimeNotifier {
    async fn subscribe(
        &self,
        tenant: &TenantId,
        config: SubscriptionConfig,
    ) -> Result<SubscriptionHandle, StorageError> {
        let subscription_id = Self::generate_subscription_id();
        let now = chrono::Utc::now().timestamp_millis();

        // Build and execute LIVE SELECT
        let live_statement = self.build_live_select(&config);
        let session = self.datastore.session().await?;

        // Note: In actual implementation, this would return a stream
        // For now, we store the subscription info
        let outcome = session.query(&live_statement, json!({})).await;

        let surreal_live_id = outcome
            .ok()
            .and_then(|o| o.rows.first().cloned())
            .and_then(|v| v.as_str().map(String::from));

        let handle = SubscriptionHandle {
            id: subscription_id.clone(),
            config: config.clone(),
            active: true,
            created_at_ms: now,
            last_notification_ms: None,
            notification_count: 0,
        };

        // Store subscription state
        {
            let mut subs = self.subscriptions.write();
            subs.insert(
                subscription_id.clone(),
                SubscriptionState {
                    handle: handle.clone(),
                    tenant_id: tenant.0.clone(),
                    surreal_live_id,
                },
            );
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.active_subscriptions += 1;
        }

        Ok(handle)
    }

    async fn unsubscribe(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError> {
        let state = {
            let mut subs = self.subscriptions.write();
            subs.remove(subscription_id)
        };

        if let Some(state) = state {
            // Kill the live query in SurrealDB
            if let Some(live_id) = state.surreal_live_id {
                let session = self.datastore.session().await?;
                let _ = session
                    .query(&format!("KILL '{}'", live_id), json!({}))
                    .await;
            }

            // Update stats
            let mut stats = self.stats.write();
            stats.active_subscriptions = stats.active_subscriptions.saturating_sub(1);

            Ok(())
        } else {
            Err(StorageError::not_found("subscription not found"))
        }
    }

    fn get_subscription(&self, subscription_id: &SubscriptionId) -> Option<SubscriptionHandle> {
        let subs = self.subscriptions.read();
        subs.get(subscription_id).map(|s| s.handle.clone())
    }

    fn list_subscriptions(&self, tenant: &TenantId) -> Vec<SubscriptionHandle> {
        let subs = self.subscriptions.read();
        subs.values()
            .filter(|s| s.tenant_id == tenant.0)
            .map(|s| s.handle.clone())
            .collect()
    }

    async fn subscribe_pattern(
        &self,
        tenant: &TenantId,
        pattern: NotificationPattern,
    ) -> Result<SubscriptionId, StorageError> {
        let subscription_id = Self::generate_subscription_id();

        // Store pattern subscription
        {
            let mut patterns = self.pattern_subscriptions.write();
            patterns.insert(
                subscription_id.clone(),
                PatternSubscriptionState {
                    id: subscription_id.clone(),
                    tenant_id: tenant.0.clone(),
                    pattern,
                    last_triggered_ms: None,
                    trigger_count: 0,
                },
            );
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.pattern_subscriptions += 1;
        }

        Ok(subscription_id)
    }

    async fn unsubscribe_pattern(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError> {
        let mut patterns = self.pattern_subscriptions.write();
        if patterns.remove(subscription_id).is_some() {
            let mut stats = self.stats.write();
            stats.pattern_subscriptions = stats.pattern_subscriptions.saturating_sub(1);
            Ok(())
        } else {
            Err(StorageError::not_found("pattern subscription not found"))
        }
    }

    async fn pause_subscription(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError> {
        let mut subs = self.subscriptions.write();
        if let Some(state) = subs.get_mut(subscription_id) {
            if state.handle.active {
                state.handle.active = false;

                let mut stats = self.stats.write();
                stats.active_subscriptions = stats.active_subscriptions.saturating_sub(1);
                stats.paused_subscriptions += 1;
            }
            Ok(())
        } else {
            Err(StorageError::not_found("subscription not found"))
        }
    }

    async fn resume_subscription(&self, subscription_id: &SubscriptionId) -> Result<(), StorageError> {
        let mut subs = self.subscriptions.write();
        if let Some(state) = subs.get_mut(subscription_id) {
            if !state.handle.active {
                state.handle.active = true;

                let mut stats = self.stats.write();
                stats.active_subscriptions += 1;
                stats.paused_subscriptions = stats.paused_subscriptions.saturating_sub(1);
            }
            Ok(())
        } else {
            Err(StorageError::not_found("subscription not found"))
        }
    }

    fn get_stats(&self) -> RealtimeStats {
        self.stats.read().clone()
    }
}

/// Event stream type for async iteration
pub struct EventStream {
    subscription_id: SubscriptionId,
    buffer: Arc<RwLock<Vec<LiveNotification>>>,
}

impl EventStream {
    /// Create a new event stream
    pub fn new(subscription_id: SubscriptionId) -> Self {
        Self {
            subscription_id,
            buffer: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get the next notification (non-blocking)
    pub fn try_next(&self) -> Option<LiveNotification> {
        let mut buffer = self.buffer.write();
        if buffer.is_empty() {
            None
        } else {
            Some(buffer.remove(0))
        }
    }

    /// Get all pending notifications
    pub fn drain(&self) -> Vec<LiveNotification> {
        let mut buffer = self.buffer.write();
        std::mem::take(&mut *buffer)
    }

    /// Check if there are pending notifications
    pub fn has_pending(&self) -> bool {
        !self.buffer.read().is_empty()
    }

    /// Get subscription ID
    pub fn subscription_id(&self) -> &SubscriptionId {
        &self.subscription_id
    }
}

/// Helper to create common subscription configurations
pub struct SubscriptionBuilder {
    config: SubscriptionConfig,
}

impl SubscriptionBuilder {
    /// Create a new builder for a table
    pub fn new(table: &str) -> Self {
        Self {
            config: SubscriptionConfig {
                table: table.to_string(),
                ..Default::default()
            },
        }
    }

    /// Subscribe to timeline events
    pub fn timeline_events() -> Self {
        Self::new("timeline_event")
    }

    /// Subscribe to awareness events
    pub fn awareness_events() -> Self {
        Self::new("awareness_event")
    }

    /// Subscribe to causal edges
    pub fn causal_edges() -> Self {
        Self::new("causal_edge")
    }

    /// Filter by tenant
    pub fn for_tenant(mut self, tenant_id: &str) -> Self {
        self.config.filter = self.config.filter.with_tenant(tenant_id);
        self
    }

    /// Filter by journey
    pub fn for_journey(mut self, journey_id: &str) -> Self {
        self.config.filter = self.config.filter.with_journey(journey_id);
        self
    }

    /// Filter by event type
    pub fn for_event_type(mut self, event_type: &str) -> Self {
        self.config.filter = self.config.filter.with_event_type(event_type);
        self
    }

    /// Include previous record in update notifications
    pub fn with_diff(mut self) -> Self {
        self.config.include_previous = true;
        self
    }

    /// Set debounce interval
    pub fn debounce(mut self, ms: u64) -> Self {
        self.config.debounce_ms = ms;
        self
    }

    /// Set rate limit
    pub fn rate_limit(mut self, per_second: u32) -> Self {
        self.config.rate_limit = per_second;
        self
    }

    /// Build the configuration
    pub fn build(self) -> SubscriptionConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_live_query_filter_build() {
        let filter = LiveQueryFilter::new()
            .with_tenant("tenant1")
            .with_journey("journey1")
            .with_event_type("message");

        let clause = filter.build_where_clause();
        assert!(clause.contains("tenant = 'tenant1'"));
        assert!(clause.contains("journey_id = 'journey1'"));
        assert!(clause.contains("event_type = 'message'"));
    }

    #[test]
    fn test_live_query_filter_empty() {
        let filter = LiveQueryFilter::new();
        let clause = filter.build_where_clause();
        assert!(clause.is_empty());
    }

    #[test]
    fn test_condition_operator_surreal() {
        assert_eq!(ConditionOperator::Equals.as_surreal_op(), "=");
        assert_eq!(ConditionOperator::GreaterThan.as_surreal_op(), ">");
        assert_eq!(ConditionOperator::Contains.as_surreal_op(), "CONTAINS");
        assert_eq!(ConditionOperator::IsNull.as_surreal_op(), "IS NONE");
    }

    #[test]
    fn test_subscription_builder() {
        let config = SubscriptionBuilder::timeline_events()
            .for_tenant("t1")
            .for_journey("j1")
            .with_diff()
            .debounce(100)
            .rate_limit(10)
            .build();

        assert_eq!(config.table, "timeline_event");
        assert!(config.include_previous);
        assert_eq!(config.debounce_ms, 100);
        assert_eq!(config.rate_limit, 10);
    }

    #[test]
    fn test_subscription_config_default() {
        let config = SubscriptionConfig::default();
        assert_eq!(config.table, "timeline_event");
        assert!(config.include_record);
        assert!(!config.include_previous);
        assert_eq!(config.debounce_ms, 0);
        assert_eq!(config.rate_limit, 0);
    }

    #[test]
    fn test_realtime_stats_default() {
        let stats = RealtimeStats::default();
        assert_eq!(stats.active_subscriptions, 0);
        assert_eq!(stats.total_notifications, 0);
        assert_eq!(stats.pattern_subscriptions, 0);
    }

    #[test]
    fn test_event_stream() {
        let stream = EventStream::new("sub-1".to_string());
        assert!(!stream.has_pending());
        assert!(stream.try_next().is_none());
        assert_eq!(stream.subscription_id(), "sub-1");
    }

    #[test]
    fn test_notification_pattern() {
        let pattern = NotificationPattern {
            name: "high_frequency".to_string(),
            description: Some("Detect high frequency events".to_string()),
            tables: vec!["timeline_event".to_string()],
            conditions: vec![PatternCondition {
                field: "count".to_string(),
                operator: ConditionOperator::GreaterThan,
                value: json!(100),
            }],
            time_window_ms: Some(60_000),
            min_occurrences: 5,
            cooldown_ms: Some(300_000),
        };

        assert_eq!(pattern.name, "high_frequency");
        assert_eq!(pattern.min_occurrences, 5);
        assert_eq!(pattern.time_window_ms, Some(60_000));
    }

    #[test]
    fn test_live_notification() {
        let notification = LiveNotification {
            subscription_id: "sub-1".to_string(),
            action: LiveAction::Create,
            record: json!({"id": "record-1", "data": "test"}),
            previous: None,
            timestamp_ms: 1234567890,
            table: "timeline_event".to_string(),
        };

        assert_eq!(notification.action, LiveAction::Create);
        assert!(notification.previous.is_none());
    }
}
