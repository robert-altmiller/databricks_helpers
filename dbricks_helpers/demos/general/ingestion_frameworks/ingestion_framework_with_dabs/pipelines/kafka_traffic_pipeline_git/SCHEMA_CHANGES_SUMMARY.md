# Kafka Traffic Pipeline - Schema Changes Summary

## Overview
Updated the kafka_traffic_pipeline to reflect new schema changes from the bronze layer where:
- **Primary Key**: Composite key consisting of `customer_id` + `latitude` + `longitude`
- **`id` field**: Now correctly represents `customer_id` (not traffic_id)
- **New Location Fields**: Added `latitude`, `longitude`, and `address` fields

## Bronze Layer Schema
The bronze table (`kafka_data_bronze`) now contains:
- `Brand` - Brand name
- `Date` - Event timestamp
- `Market` - Market identifier (e.g., CA, US)
- `address` - Physical address
- `email` - Customer email
- `id` - **Customer ID** (primary key component)
- `latitude` - Latitude coordinate (primary key component)
- `longitude` - Longitude coordinate (primary key component)
- `traffic_count` - Traffic count value
- `topic` - Kafka topic name
- `partition` - Kafka partition
- `offset` - Kafka offset
- `kafka_timestamp` - Kafka message timestamp
- `ingestion_timestamp` - Data ingestion timestamp

## Changes Made

### 1. Bronze to Silver - `traffic_data_clean.py`

**Key Changes:**
- Renamed `id` to `customer_id` for clarity
- Added `latitude`, `longitude`, and `address` fields
- Created composite `location_key` = `customer_id_latitude_longitude`
- Added location validation checks
- Updated data quality score to include location validity (20 points)
- Updated filters to require customer_id, latitude, and longitude

**New Fields in Silver:**
- `customer_id` (renamed from id)
- `latitude`
- `longitude`
- `address`
- `location_key` (composite key)
- `location_valid` (boolean validation flag)

### 2. Bronze to Silver - `user_engagement_summary.py`

**Key Changes:**
- Changed aggregation from `email` to `customer_id`
- Added location-based metrics:
  - `unique_locations` - Count of distinct locations per customer
  - `addresses_visited` - List of addresses
  - `avg_latitude` / `avg_longitude` - Average location coordinates
- Updated engagement score calculation to include location diversity (15 points)
- Renamed all "user" references to "customer" for consistency
- Updated primary key to `customer_id`

**New Engagement Scoring (0-100):**
- Total events: 30%
- Brands engaged: 25%
- Markets engaged: 20%
- **Unique locations: 15%** (NEW)
- Active days: 10%

**Target Table:** Renamed to `customer_engagement_summary`

### 3. Silver to Gold - `traffic_analytics_aggregation.py`

**Key Changes:**
- Changed from `traffic_id` to `location_key` for event counting
- Changed from `unique_users` to `unique_customers`
- Added location-based metrics:
  - `unique_locations` - Distinct location count
  - `avg_latitude` / `avg_longitude` - Average coordinates
  - `location_valid_rate` - Percentage of valid locations
  - `locations_per_customer` - Average locations per customer
- Updated all aggregations to use `customer_id` instead of email
- Added `addresses` to collected arrays
- Updated hourly and day-of-week pattern analysis with location metrics

**New Metrics:**
- `unique_customers` (replacing unique_users)
- `unique_locations`
- `locations_per_customer`
- `events_per_customer` (replacing events_per_user)
- `avg_traffic_per_customer` (replacing avg_traffic_per_user)
- `location_valid_rate`

### 4. Configuration - `config.json`

**Key Changes:**
- Updated `z_order_by` for step 1: `["customer_id", "latitude", "longitude"]`
- Added `primary_key` field: `["customer_id", "latitude", "longitude"]`
- Updated step 2 target table name: `customer_engagement_summary`
- Updated `z_order_by` for step 2: `["customer_id", "engagement_score"]`
- Added descriptions for all transformation steps
- Updated source tables reference in step 3

## Data Quality Improvements

### Silver Layer Validation
Now enforces:
1. Valid traffic count (> 0)
2. Non-null Brand
3. Non-null Market
4. Non-null customer_id
5. Non-null latitude
6. Non-null longitude
7. Valid email format (optional)

### Quality Score Components
- Email validity: 25 points
- Traffic count > 0: 25 points
- Brand present: 15 points
- Market present: 15 points
- **Location valid: 20 points** (NEW)

## Migration Notes

### Breaking Changes
1. **Primary Key Changed**: From `traffic_id` to composite key `(customer_id, latitude, longitude)`
2. **Table Renamed**: `user_engagement_summary` → `customer_engagement_summary`
3. **Field Renamed**: `id` → `customer_id` in silver/gold layers
4. **Aggregation Key Changed**: From `email` to `customer_id` for customer aggregations

### Backward Compatibility
- None - This is a schema-breaking change
- Downstream consumers must update to use new field names and composite key
- Gold layer queries must reference `customer_id` instead of `traffic_id`

## Performance Optimizations

### Z-Ordering
- **Silver Traffic Table**: Ordered by `customer_id`, `latitude`, `longitude` for location-based queries
- **Silver Customer Table**: Ordered by `customer_id`, `engagement_score` for customer lookups
- **Gold Traffic Table**: Ordered by `Brand`, `Market` for business analytics

### Partitioning
- **Silver Traffic**: Partitioned by `Brand`, `Market`
- **Silver Customer**: Partitioned by `user_segment`
- **Gold Traffic**: Partitioned by `Brand`, `event_date_str`

## Usage Examples

### Query by Customer and Location
```sql
SELECT *
FROM jom_dab_framework.silver_db.traffic_data_cleaned
WHERE customer_id = '2455475a-65ed-4931-a562-5461cd635b9b'
  AND latitude = 63.52066
  AND longitude = 28.060454
```

### Customer Location Analysis
```sql
SELECT 
  customer_id,
  email,
  unique_locations,
  addresses_visited_str,
  engagement_score
FROM jom_dab_framework.silver_db.customer_engagement_summary
WHERE unique_locations > 3
ORDER BY engagement_score DESC
```

### Gold Layer Analytics
```sql
SELECT 
  Brand,
  Market,
  event_date_str,
  unique_customers,
  unique_locations,
  locations_per_customer,
  total_traffic
FROM jom_dab_framework.gold_db.traffic_analytics
WHERE Brand = 'Athleta'
ORDER BY event_date_str DESC
```

## Next Steps

1. **Test the Pipeline**: Run the complete pipeline to validate transformations
2. **Update Downstream Systems**: Modify any queries/reports using the old schema
3. **Monitor Performance**: Check query performance with new composite key and z-ordering
4. **Update Documentation**: Update any API documentation or data dictionaries
5. **Notify Stakeholders**: Inform data consumers about schema changes

## Rollback Plan

If issues arise:
1. Revert to previous notebook versions
2. Update config.json to use old field names and z-order keys
3. Redeploy the pipeline
4. Note: Data already processed with new schema may need reprocessing

---
**Last Updated**: December 3, 2025
**Updated By**: Schema Migration - Composite Key Implementation


