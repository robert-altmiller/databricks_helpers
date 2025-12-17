# Kafka to Lakebase Pipeline

Example streaming pipeline that ingests data from Kafka to PostgreSQL (Lakebase).

## Overview

This pipeline demonstrates:
- Kafka streaming ingestion
- PostgreSQL integration
- Multi-task pipeline with shared cluster
- Customer statistics generation

## Configuration

See `config/config.json` for full configuration.

### Key Settings

- **Pipeline Type**: `streaming_to_postgres`
- **Kafka Topic**: Configure in config.json
- **PostgreSQL**: Lakebase instance connection
- **Shared Cluster**: Efficient resource usage

## Notebooks

1. **kafka_to_postgres.py** - Main streaming ingestion from Kafka to Postgres
2. **customer_stats_generator.py** - Generate customer statistics periodically
3. **lakebase_helpers.py** - Helper functions for Lakebase operations

## Deployment

```bash
# From project root
./scripts/deploy.sh kafka_to_lakebase dev

# Run the pipeline
cd pipelines/kafka_to_lakebase
databricks bundle run kafka_to_postgres_pipeline -t dev
```

## Monitoring

- Email notifications on failure
- Streaming backlog alerts
- PostgreSQL connection monitoring

## Notes

This is a working example copied from the original dab_framework.
Customize it for your use case by modifying the config.json and notebooks.

