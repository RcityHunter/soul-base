//! Spatial Query Support for SurrealDB
//!
//! This module provides spatial/geographic query capabilities leveraging
//! SurrealDB's native geometry types and spatial functions for location-based
//! context analysis and geospatial queries.

use super::datastore::SurrealDatastore;
use crate::errors::StorageError;
use crate::spi::datastore::Datastore;
use crate::spi::query::QueryExecutor;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use soulbase_types::prelude::TenantId;

/// Earth radius in meters
pub const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

/// A geographic point (latitude, longitude)
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    /// Latitude in degrees (-90 to 90)
    pub lat: f64,
    /// Longitude in degrees (-180 to 180)
    pub lon: f64,
}

impl GeoPoint {
    /// Create a new GeoPoint
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }

    /// Convert to SurrealDB geometry format
    pub fn to_surreal_geometry(&self) -> Value {
        json!({
            "type": "Point",
            "coordinates": [self.lon, self.lat]
        })
    }

    /// Parse from SurrealDB geometry format
    pub fn from_surreal_geometry(value: &Value) -> Option<Self> {
        let coords = value.get("coordinates")?.as_array()?;
        let lon = coords.first()?.as_f64()?;
        let lat = coords.get(1)?.as_f64()?;
        Some(Self { lat, lon })
    }

    /// Calculate distance to another point in meters using Haversine formula
    pub fn distance_to(&self, other: &GeoPoint) -> f64 {
        let lat1_rad = self.lat.to_radians();
        let lat2_rad = other.lat.to_radians();
        let delta_lat = (other.lat - self.lat).to_radians();
        let delta_lon = (other.lon - self.lon).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2)
            + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().asin();

        EARTH_RADIUS_METERS * c
    }

    /// Check if point is within a bounding box
    pub fn within_bounds(&self, bounds: &GeoBoundingBox) -> bool {
        self.lat >= bounds.south
            && self.lat <= bounds.north
            && self.lon >= bounds.west
            && self.lon <= bounds.east
    }

    /// Calculate bearing to another point in degrees (0-360)
    pub fn bearing_to(&self, other: &GeoPoint) -> f64 {
        let lat1 = self.lat.to_radians();
        let lat2 = other.lat.to_radians();
        let delta_lon = (other.lon - self.lon).to_radians();

        let y = delta_lon.sin() * lat2.cos();
        let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * delta_lon.cos();

        let bearing = y.atan2(x).to_degrees();
        (bearing + 360.0) % 360.0
    }

    /// Get a point at a given distance and bearing from this point
    pub fn destination_point(&self, distance_meters: f64, bearing_degrees: f64) -> GeoPoint {
        let bearing_rad = bearing_degrees.to_radians();
        let angular_distance = distance_meters / EARTH_RADIUS_METERS;

        let lat1 = self.lat.to_radians();
        let lon1 = self.lon.to_radians();

        let lat2 = (lat1.sin() * angular_distance.cos()
            + lat1.cos() * angular_distance.sin() * bearing_rad.cos())
        .asin();

        let lon2 = lon1
            + (bearing_rad.sin() * angular_distance.sin() * lat1.cos())
                .atan2(angular_distance.cos() - lat1.sin() * lat2.sin());

        GeoPoint {
            lat: lat2.to_degrees(),
            lon: lon2.to_degrees(),
        }
    }
}

impl Default for GeoPoint {
    fn default() -> Self {
        Self { lat: 0.0, lon: 0.0 }
    }
}

/// A geographic bounding box
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoBoundingBox {
    /// Northern latitude boundary
    pub north: f64,
    /// Southern latitude boundary
    pub south: f64,
    /// Eastern longitude boundary
    pub east: f64,
    /// Western longitude boundary
    pub west: f64,
}

impl GeoBoundingBox {
    /// Create a new bounding box
    pub fn new(north: f64, south: f64, east: f64, west: f64) -> Self {
        Self {
            north,
            south,
            east,
            west,
        }
    }

    /// Create a bounding box from center point and radius
    pub fn from_center_radius(center: GeoPoint, radius_meters: f64) -> Self {
        // Approximate degrees per meter at the equator
        let lat_delta = (radius_meters / EARTH_RADIUS_METERS).to_degrees();
        let lon_delta = lat_delta / center.lat.to_radians().cos().max(0.01);

        Self {
            north: center.lat + lat_delta,
            south: center.lat - lat_delta,
            east: center.lon + lon_delta,
            west: center.lon - lon_delta,
        }
    }

    /// Convert to SurrealDB polygon format
    pub fn to_surreal_geometry(&self) -> Value {
        json!({
            "type": "Polygon",
            "coordinates": [[
                [self.west, self.south],
                [self.east, self.south],
                [self.east, self.north],
                [self.west, self.north],
                [self.west, self.south]  // Close the polygon
            ]]
        })
    }

    /// Get center point
    pub fn center(&self) -> GeoPoint {
        GeoPoint {
            lat: (self.north + self.south) / 2.0,
            lon: (self.east + self.west) / 2.0,
        }
    }

    /// Check if bounding boxes intersect
    pub fn intersects(&self, other: &GeoBoundingBox) -> bool {
        self.west <= other.east
            && self.east >= other.west
            && self.south <= other.north
            && self.north >= other.south
    }

    /// Expand the bounding box by a margin (in meters)
    pub fn expand(&self, margin_meters: f64) -> Self {
        let center = self.center();
        let margin_deg = (margin_meters / EARTH_RADIUS_METERS).to_degrees();
        let margin_lon = margin_deg / center.lat.to_radians().cos().max(0.01);

        Self {
            north: self.north + margin_deg,
            south: self.south - margin_deg,
            east: self.east + margin_lon,
            west: self.west - margin_lon,
        }
    }
}

/// A geographic polygon
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoPolygon {
    /// Exterior ring (must be closed - first point equals last)
    pub exterior: Vec<GeoPoint>,
    /// Interior rings (holes)
    pub holes: Vec<Vec<GeoPoint>>,
}

impl GeoPolygon {
    /// Create a new polygon from exterior ring
    pub fn new(exterior: Vec<GeoPoint>) -> Self {
        Self {
            exterior,
            holes: Vec::new(),
        }
    }

    /// Add a hole to the polygon
    pub fn with_hole(mut self, hole: Vec<GeoPoint>) -> Self {
        self.holes.push(hole);
        self
    }

    /// Convert to SurrealDB geometry format
    pub fn to_surreal_geometry(&self) -> Value {
        let mut rings = vec![self
            .exterior
            .iter()
            .map(|p| vec![p.lon, p.lat])
            .collect::<Vec<_>>()];

        for hole in &self.holes {
            rings.push(hole.iter().map(|p| vec![p.lon, p.lat]).collect());
        }

        json!({
            "type": "Polygon",
            "coordinates": rings
        })
    }

    /// Calculate approximate area in square meters
    pub fn area(&self) -> f64 {
        // Using Shoelace formula with geodetic correction
        if self.exterior.len() < 3 {
            return 0.0;
        }

        let mut area = 0.0;
        let n = self.exterior.len();

        for i in 0..n {
            let j = (i + 1) % n;
            let lat1 = self.exterior[i].lat.to_radians();
            let lat2 = self.exterior[j].lat.to_radians();
            let delta_lon = (self.exterior[j].lon - self.exterior[i].lon).to_radians();

            area += delta_lon * (2.0 + lat1.sin() + lat2.sin());
        }

        (area * EARTH_RADIUS_METERS * EARTH_RADIUS_METERS / 2.0).abs()
    }

    /// Get bounding box of polygon
    pub fn bounding_box(&self) -> GeoBoundingBox {
        let mut north = f64::NEG_INFINITY;
        let mut south = f64::INFINITY;
        let mut east = f64::NEG_INFINITY;
        let mut west = f64::INFINITY;

        for point in &self.exterior {
            north = north.max(point.lat);
            south = south.min(point.lat);
            east = east.max(point.lon);
            west = west.min(point.lon);
        }

        GeoBoundingBox {
            north,
            south,
            east,
            west,
        }
    }
}

/// Spatial query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialResult {
    /// Record ID
    pub id: String,
    /// Location of the record
    pub location: GeoPoint,
    /// Distance from query point (if applicable)
    pub distance_meters: Option<f64>,
    /// Record data
    pub data: Value,
}

/// Spatial query configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialQueryConfig {
    /// Maximum results to return
    pub limit: usize,
    /// Minimum distance filter (meters)
    pub min_distance: Option<f64>,
    /// Maximum distance filter (meters)
    pub max_distance: Option<f64>,
    /// Sort by distance
    pub sort_by_distance: bool,
    /// Additional filter conditions
    pub filter: Option<Value>,
}

impl Default for SpatialQueryConfig {
    fn default() -> Self {
        Self {
            limit: 100,
            min_distance: None,
            max_distance: None,
            sort_by_distance: true,
            filter: None,
        }
    }
}

/// Spatial query executor trait
#[async_trait]
pub trait SpatialQueryExecutor: Send + Sync {
    /// Find records near a point
    async fn find_near(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        center: GeoPoint,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError>;

    /// Find records within a radius
    async fn find_within_radius(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        center: GeoPoint,
        radius_meters: f64,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError>;

    /// Find records within a bounding box
    async fn find_within_bounds(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        bounds: GeoBoundingBox,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError>;

    /// Find records within a polygon
    async fn find_within_polygon(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        polygon: GeoPolygon,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError>;

    /// Store a location for a record
    async fn store_location(
        &self,
        tenant: &TenantId,
        table: &str,
        record_id: &str,
        location_field: &str,
        location: GeoPoint,
    ) -> Result<(), StorageError>;

    /// Update a location for a record
    async fn update_location(
        &self,
        tenant: &TenantId,
        table: &str,
        record_id: &str,
        location_field: &str,
        location: GeoPoint,
    ) -> Result<(), StorageError>;

    /// Get location for a record
    async fn get_location(
        &self,
        tenant: &TenantId,
        table: &str,
        record_id: &str,
        location_field: &str,
    ) -> Result<Option<GeoPoint>, StorageError>;

    /// Calculate distance between two records
    async fn distance_between(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        record_id_a: &str,
        record_id_b: &str,
    ) -> Result<f64, StorageError>;
}

/// SurrealDB implementation of SpatialQueryExecutor
#[derive(Clone)]
pub struct SurrealSpatialQueryExecutor {
    datastore: SurrealDatastore,
}

impl SurrealSpatialQueryExecutor {
    /// Create a new SurrealSpatialQueryExecutor
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

    fn record_key(&self, table: &str, id: &str) -> String {
        let prefix = format!("{}:", table);
        id.strip_prefix(&prefix).unwrap_or(id).to_string()
    }

    fn build_filter_clause(&self, tenant: &TenantId, filter: Option<&Value>) -> String {
        let mut conditions = vec![format!("tenant = '{}'", tenant.0)];

        if let Some(Value::Object(filter_map)) = filter {
            for (key, value) in filter_map {
                match value {
                    Value::String(s) => conditions.push(format!("{} = '{}'", key, s)),
                    Value::Number(n) => conditions.push(format!("{} = {}", key, n)),
                    Value::Bool(b) => conditions.push(format!("{} = {}", key, b)),
                    _ => {}
                }
            }
        }

        conditions.join(" AND ")
    }

    fn parse_spatial_results(
        &self,
        rows: Vec<Value>,
        center: Option<GeoPoint>,
        location_field: &str,
    ) -> Vec<SpatialResult> {
        rows.into_iter()
            .filter_map(|row| {
                let id = row.get("id")?.as_str()?.to_string();
                let location_value = row.get(location_field)?;
                let location = GeoPoint::from_surreal_geometry(location_value)?;

                let distance_meters = center.map(|c| c.distance_to(&location));

                Some(SpatialResult {
                    id,
                    location,
                    distance_meters,
                    data: row,
                })
            })
            .collect()
    }
}

#[async_trait]
impl SpatialQueryExecutor for SurrealSpatialQueryExecutor {
    async fn find_near(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        center: GeoPoint,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError> {
        let filter_clause = self.build_filter_clause(tenant, config.filter.as_ref());
        let center_geom = center.to_surreal_geometry();

        // Build distance conditions
        let mut distance_conditions = Vec::new();
        if let Some(min) = config.min_distance {
            distance_conditions.push(format!(
                "geo::distance({}, $center) >= {}",
                location_field, min
            ));
        }
        if let Some(max) = config.max_distance {
            distance_conditions.push(format!(
                "geo::distance({}, $center) <= {}",
                location_field, max
            ));
        }

        let distance_clause = if distance_conditions.is_empty() {
            String::new()
        } else {
            format!(" AND {}", distance_conditions.join(" AND "))
        };

        let order_clause = if config.sort_by_distance {
            format!("ORDER BY geo::distance({}, $center) ASC", location_field)
        } else {
            String::new()
        };

        let statement = format!(
            r#"
            SELECT *, type::string(id) AS id, geo::distance({}, $center) AS _distance
            FROM {}
            WHERE {} {}
            {}
            LIMIT $limit;
            "#,
            location_field, table, filter_clause, distance_clause, order_clause
        );

        let rows = self
            .run_query(
                &statement,
                json!({
                    "center": center_geom,
                    "limit": config.limit,
                }),
            )
            .await?;

        Ok(self.parse_spatial_results(rows, Some(center), location_field))
    }

    async fn find_within_radius(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        center: GeoPoint,
        radius_meters: f64,
        mut config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError> {
        config.max_distance = Some(radius_meters);
        self.find_near(tenant, table, location_field, center, config)
            .await
    }

    async fn find_within_bounds(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        bounds: GeoBoundingBox,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError> {
        let filter_clause = self.build_filter_clause(tenant, config.filter.as_ref());
        let bounds_geom = bounds.to_surreal_geometry();

        let statement = format!(
            r#"
            SELECT *, type::string(id) AS id
            FROM {}
            WHERE {}
              AND geo::contains($bounds, {})
            LIMIT $limit;
            "#,
            table, filter_clause, location_field
        );

        let rows = self
            .run_query(
                &statement,
                json!({
                    "bounds": bounds_geom,
                    "limit": config.limit,
                }),
            )
            .await?;

        Ok(self.parse_spatial_results(rows, Some(bounds.center()), location_field))
    }

    async fn find_within_polygon(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        polygon: GeoPolygon,
        config: SpatialQueryConfig,
    ) -> Result<Vec<SpatialResult>, StorageError> {
        let filter_clause = self.build_filter_clause(tenant, config.filter.as_ref());
        let polygon_geom = polygon.to_surreal_geometry();
        let center = polygon.bounding_box().center();

        let statement = format!(
            r#"
            SELECT *, type::string(id) AS id
            FROM {}
            WHERE {}
              AND geo::contains($polygon, {})
            LIMIT $limit;
            "#,
            table, filter_clause, location_field
        );

        let rows = self
            .run_query(
                &statement,
                json!({
                    "polygon": polygon_geom,
                    "limit": config.limit,
                }),
            )
            .await?;

        Ok(self.parse_spatial_results(rows, Some(center), location_field))
    }

    async fn store_location(
        &self,
        tenant: &TenantId,
        table: &str,
        record_id: &str,
        location_field: &str,
        location: GeoPoint,
    ) -> Result<(), StorageError> {
        let key = self.record_key(table, record_id);
        let location_geom = location.to_surreal_geometry();

        let statement = format!(
            r#"
            UPDATE type::thing($table, $key)
            SET {} = $location
            WHERE tenant = $tenant
            RETURN NONE;
            "#,
            location_field
        );

        self.run_query(
            &statement,
            json!({
                "table": table,
                "key": key,
                "tenant": tenant.0,
                "location": location_geom,
            }),
        )
        .await?;

        Ok(())
    }

    async fn update_location(
        &self,
        tenant: &TenantId,
        table: &str,
        record_id: &str,
        location_field: &str,
        location: GeoPoint,
    ) -> Result<(), StorageError> {
        // Same as store_location for SurrealDB
        self.store_location(tenant, table, record_id, location_field, location)
            .await
    }

    async fn get_location(
        &self,
        tenant: &TenantId,
        table: &str,
        record_id: &str,
        location_field: &str,
    ) -> Result<Option<GeoPoint>, StorageError> {
        let key = self.record_key(table, record_id);

        let statement = format!(
            r#"
            SELECT {}
            FROM type::thing($table, $key)
            WHERE tenant = $tenant
            LIMIT 1;
            "#,
            location_field
        );

        let rows = self
            .run_query(
                &statement,
                json!({
                    "table": table,
                    "key": key,
                    "tenant": tenant.0,
                }),
            )
            .await?;

        if let Some(row) = rows.into_iter().next() {
            if let Some(location_value) = row.get(location_field) {
                return Ok(GeoPoint::from_surreal_geometry(location_value));
            }
        }

        Ok(None)
    }

    async fn distance_between(
        &self,
        tenant: &TenantId,
        table: &str,
        location_field: &str,
        record_id_a: &str,
        record_id_b: &str,
    ) -> Result<f64, StorageError> {
        let key_a = self.record_key(table, record_id_a);
        let key_b = self.record_key(table, record_id_b);

        let statement = format!(
            r#"
            LET $a = (SELECT {} FROM type::thing($table, $key_a) WHERE tenant = $tenant LIMIT 1);
            LET $b = (SELECT {} FROM type::thing($table, $key_b) WHERE tenant = $tenant LIMIT 1);
            RETURN geo::distance($a[0].{}, $b[0].{});
            "#,
            location_field, location_field, location_field, location_field
        );

        let rows = self
            .run_query(
                &statement,
                json!({
                    "table": table,
                    "key_a": key_a,
                    "key_b": key_b,
                    "tenant": tenant.0,
                }),
            )
            .await?;

        rows.first()
            .and_then(|v| v.as_f64())
            .ok_or_else(|| StorageError::not_found("could not calculate distance"))
    }
}

/// Geohash utilities for spatial indexing
pub mod geohash {
    use super::GeoPoint;

    const BASE32: &[char] = &[
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j',
        'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];

    /// Encode a point to geohash string
    pub fn encode(point: GeoPoint, precision: usize) -> String {
        let mut lat_range = (-90.0, 90.0);
        let mut lon_range = (-180.0, 180.0);
        let mut hash = String::new();
        let mut bit = 0;
        let mut ch = 0u8;
        let mut even_bit = true;

        while hash.len() < precision {
            if even_bit {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if point.lon >= mid {
                    ch |= 1 << (4 - bit);
                    lon_range.0 = mid;
                } else {
                    lon_range.1 = mid;
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if point.lat >= mid {
                    ch |= 1 << (4 - bit);
                    lat_range.0 = mid;
                } else {
                    lat_range.1 = mid;
                }
            }

            even_bit = !even_bit;
            bit += 1;

            if bit == 5 {
                hash.push(BASE32[ch as usize]);
                bit = 0;
                ch = 0;
            }
        }

        hash
    }

    /// Get neighboring geohashes
    pub fn neighbors(geohash: &str) -> Vec<String> {
        // Simplified: just return the hash itself and prefix matches
        // A full implementation would calculate actual neighbors
        vec![geohash.to_string()]
    }

    /// Get bounding box for a geohash
    pub fn decode_bounds(geohash: &str) -> Option<super::GeoBoundingBox> {
        if geohash.is_empty() {
            return None;
        }

        let mut lat_range = (-90.0, 90.0);
        let mut lon_range = (-180.0, 180.0);
        let mut even_bit = true;

        for c in geohash.chars() {
            let idx = BASE32.iter().position(|&x| x == c)?;
            for bit in (0..5).rev() {
                let mask = 1 << bit;
                if even_bit {
                    let mid = (lon_range.0 + lon_range.1) / 2.0;
                    if idx & mask != 0 {
                        lon_range.0 = mid;
                    } else {
                        lon_range.1 = mid;
                    }
                } else {
                    let mid = (lat_range.0 + lat_range.1) / 2.0;
                    if idx & mask != 0 {
                        lat_range.0 = mid;
                    } else {
                        lat_range.1 = mid;
                    }
                }
                even_bit = !even_bit;
            }
        }

        Some(super::GeoBoundingBox {
            north: lat_range.1,
            south: lat_range.0,
            east: lon_range.1,
            west: lon_range.0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_point_distance() {
        // New York City
        let nyc = GeoPoint::new(40.7128, -74.0060);
        // Los Angeles
        let la = GeoPoint::new(34.0522, -118.2437);

        let distance = nyc.distance_to(&la);
        // Should be approximately 3940 km
        assert!((distance - 3_940_000.0).abs() < 50_000.0);
    }

    #[test]
    fn test_geo_point_within_bounds() {
        let point = GeoPoint::new(40.7128, -74.0060);
        let bounds = GeoBoundingBox::new(41.0, 40.0, -73.0, -75.0);

        assert!(point.within_bounds(&bounds));

        let outside_point = GeoPoint::new(35.0, -74.0);
        assert!(!outside_point.within_bounds(&bounds));
    }

    #[test]
    fn test_geo_point_bearing() {
        let point_a = GeoPoint::new(0.0, 0.0);
        let point_b = GeoPoint::new(1.0, 0.0); // Due north

        let bearing = point_a.bearing_to(&point_b);
        assert!((bearing - 0.0).abs() < 1.0);

        let point_c = GeoPoint::new(0.0, 1.0); // Due east
        let bearing_east = point_a.bearing_to(&point_c);
        assert!((bearing_east - 90.0).abs() < 1.0);
    }

    #[test]
    fn test_bounding_box_from_center() {
        let center = GeoPoint::new(40.7128, -74.0060);
        let bounds = GeoBoundingBox::from_center_radius(center, 10_000.0); // 10km

        assert!(bounds.north > center.lat);
        assert!(bounds.south < center.lat);
        assert!(bounds.east > center.lon);
        assert!(bounds.west < center.lon);
    }

    #[test]
    fn test_bounding_box_intersects() {
        let box_a = GeoBoundingBox::new(42.0, 40.0, -72.0, -75.0);
        let box_b = GeoBoundingBox::new(41.5, 39.5, -73.0, -76.0);

        assert!(box_a.intersects(&box_b));

        let box_c = GeoBoundingBox::new(50.0, 48.0, -70.0, -72.0);
        assert!(!box_a.intersects(&box_c));
    }

    #[test]
    fn test_geo_polygon_bounding_box() {
        let polygon = GeoPolygon::new(vec![
            GeoPoint::new(40.0, -75.0),
            GeoPoint::new(40.0, -73.0),
            GeoPoint::new(42.0, -73.0),
            GeoPoint::new(42.0, -75.0),
            GeoPoint::new(40.0, -75.0),
        ]);

        let bounds = polygon.bounding_box();
        assert_eq!(bounds.north, 42.0);
        assert_eq!(bounds.south, 40.0);
        assert_eq!(bounds.east, -73.0);
        assert_eq!(bounds.west, -75.0);
    }

    #[test]
    fn test_geo_point_surreal_geometry() {
        let point = GeoPoint::new(40.7128, -74.0060);
        let geom = point.to_surreal_geometry();

        assert_eq!(geom["type"], "Point");
        let coords = geom["coordinates"].as_array().unwrap();
        assert_eq!(coords[0].as_f64().unwrap(), -74.0060);
        assert_eq!(coords[1].as_f64().unwrap(), 40.7128);

        let parsed = GeoPoint::from_surreal_geometry(&geom).unwrap();
        assert_eq!(parsed.lat, 40.7128);
        assert_eq!(parsed.lon, -74.0060);
    }

    #[test]
    fn test_spatial_query_config_default() {
        let config = SpatialQueryConfig::default();
        assert_eq!(config.limit, 100);
        assert!(config.sort_by_distance);
        assert!(config.min_distance.is_none());
        assert!(config.max_distance.is_none());
    }

    #[test]
    fn test_geohash_encode() {
        let point = GeoPoint::new(40.7128, -74.0060);
        let hash = geohash::encode(point, 6);
        assert_eq!(hash.len(), 6);
        assert!(hash.chars().all(|c| c.is_alphanumeric()));
    }

    #[test]
    fn test_geohash_decode_bounds() {
        let bounds = geohash::decode_bounds("dr5r").unwrap();
        assert!(bounds.north > bounds.south);
        assert!(bounds.east > bounds.west);
    }

    #[test]
    fn test_destination_point() {
        let start = GeoPoint::new(0.0, 0.0);
        let dest = start.destination_point(111_000.0, 0.0); // ~1 degree north

        assert!((dest.lat - 1.0).abs() < 0.1);
        assert!(dest.lon.abs() < 0.1);
    }
}
