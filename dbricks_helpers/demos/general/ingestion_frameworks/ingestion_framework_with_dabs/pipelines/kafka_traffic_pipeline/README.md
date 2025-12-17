# Kafka Traffic Pipeline

## Overview
This pipeline processes Kafka traffic data from Bronze to Gold layer, creating cleaned data and business analytics.

## Pipeline Architecture

```
                Bronze Layer (Kafka)
                        ↓
        ┌───────────────┴───────────────┐
        ↓                               ↓
Silver Layer                    Silver Layer
(Traffic Data Cleaned)    (User Engagement Summary)
        └───────────────┬───────────────┘
                        ↓
            Gold Layer (Analytics & Aggregations)
```

## Data Flow

### 1. Bronze Layer
- **Source**: `jom_dab_framework.kafka_bronze.kafka_data_bronze`
- **Description**: Raw Kafka traffic data ingested from Kafka topics
- **Schema**:
  - Brand (STRING)
  - Date (TIMESTAMP)
  - Market (STRING)
  - email (STRING)
  - id (STRING)
  - traffic_count (INT)
  - topic (STRING)
  - partition (INT)
  - offset (BIGINT)
  - kafka_timestamp (TIMESTAMP)
  - ingestion_timestamp (TIMESTAMP)

### 2. Silver Layer
- **Target**: `jom_dab_framework.silver_db.traffic_data_cleaned`
- **Transformations**:
  - Email validation and cleaning
  - Time dimension extraction (hour, day of week, date string)
  - Data quality scoring
  - Invalid record filtering
  - Processing latency calculation
- **Partitioned By**: Brand, Market

### 3. Gold Layer
- **Main Table**: `jom_dab_framework.gold_db.traffic_analytics`
  - Daily aggregations by Brand and Market
  - Traffic metrics (total, average per event, per user)
  - User engagement metrics
  - Data quality metrics
  - **Partitioned By**: Brand, event_date_str

- **Hourly Patterns**: `jom_dab_framework.gold_db.traffic_hourly_patterns`
  - Traffic patterns by hour of day
  - Helps identify peak traffic hours

- **Day of Week Patterns**: `jom_dab_framework.gold_db.traffic_dow_patterns`
  - Traffic patterns by day of week
  - Helps identify weekly trends

## Key Metrics

### Traffic Metrics
- Total traffic count
- Average traffic per event
- Total events
- Unique users
- Events per user
- Average traffic per user

### Quality Metrics
- Data quality score (0-100)
- Email validation rate
- Processing latency

### Kafka Metrics
- Partitions used
- Offset ranges
- Kafka to ingestion latency

## Execution Sequence

1. **Step 1: Traffic Data Clean (Bronze → Silver)**
   - Notebook: `notebooks/bronze_to_silver/traffic_data_clean.py`
   - Reads from Kafka bronze table
   - Cleans and validates data
   - Writes to silver layer

2. **Step 2: Traffic Analytics Aggregation (Silver → Gold)**
   - Notebook: `notebooks/silver_to_gold/traffic_analytics_aggregation.py`
   - Reads from silver layer
   - Creates aggregations and analytics
   - Writes to gold layer (3 tables)

## Deployment

### Using Databricks Asset Bundles (DAB)

```bash
# Validate the bundle
databricks bundle validate -t dev

# Deploy to dev environment
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run kafka_traffic_pipeline -t dev
```

### Using the Framework Scripts

```bash
# From the dab_framework directory
./deploy.sh kafka_traffic_pipeline dev
```

## Configuration

All pipeline configuration is in `config.json`:
- Database names
- Table names
- Partition keys
- Optimization settings
- Dependencies

## Monitoring

The pipeline updates the checkpoint table at each step:
- Table: `jom_dab_framework.framework_metadata.checkpoints`
- Tracks processing timestamps and record counts

## Data Quality

The pipeline implements comprehensive data quality checks:
- Email format validation
- Non-null Brand and Market requirements
- Positive traffic count validation
- Quality scoring (0-100 scale)

## Performance Optimization

- Partitioning by Brand and Market for efficient queries
- Z-ordering on key columns
- Auto-optimization enabled
- Incremental processing support

## Usage Examples

### Query Daily Traffic by Brand
```sql
SELECT 
    Brand,
    Market,
    event_date_str,
    total_traffic,
    unique_users,
    avg_traffic_per_user
FROM jom_dab_framework.gold_db.traffic_analytics
WHERE Brand = 'Athleta'
ORDER BY event_date_str DESC;
```

### Find Peak Traffic Hours
```sql
SELECT 
    Brand,
    Market,
    hour_of_day,
    total_traffic
FROM jom_dab_framework.gold_db.traffic_hourly_patterns
ORDER BY total_traffic DESC
LIMIT 10;
```

### Analyze Day of Week Patterns
```sql
SELECT 
    Brand,
    Market,
    day_name,
    total_traffic,
    unique_users
FROM jom_dab_framework.gold_db.traffic_dow_patterns
ORDER BY Brand, day_of_week;
```

## Troubleshooting

### Common Issues

1. **Source table not found**
   - Ensure Kafka ingestion pipeline has run
   - Check table name: `jom_dab_framework.kafka_bronze.kafka_data_bronze`

2. **Low data quality scores**
   - Review email validation rules
   - Check for null values in Brand/Market fields

3. **Performance issues**
   - Verify partitioning is working correctly
   - Check for data skew in Brand/Market partitions

## Contact

For questions or issues, contact: data-team@company.com

