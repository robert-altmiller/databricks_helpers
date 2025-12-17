# Loyalty 2.0 DAB Framework - Architecture Document

**Version:** 1.0  
**Date:** December 10, 2025  
**Status:** Production Ready

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Component Architecture](#component-architecture)
4. [Data Flow Architecture](#data-flow-architecture)
5. [Deployment Architecture](#deployment-architecture)
6. [Pipeline Types](#pipeline-types)
7. [Configuration Architecture](#configuration-architecture)
8. [Security Architecture](#security-architecture)

---

## 🎯 Overview

### Purpose
Production-ready Databricks Asset Bundle (DAB) framework for deploying and managing data pipelines in the Loyalty 2.0 platform.

### Key Features
- ✅ **Multi-Pipeline Support**: Streaming and Batch SQL pipelines
- ✅ **Auto-Detection**: Automatic pipeline type identification
- ✅ **Multi-Environment**: Dev, Staging, Production isolation
- ✅ **Clean Naming**: `[Loyalty2.0]` branded job names
- ✅ **One-Command Deploy**: Simple deployment workflow

---

## 🏗️ System Architecture

### High-Level System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    LOYALTY 2.0 DAB FRAMEWORK                        │
│                     (Production Environment)                         │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        │                          │                          │
        ▼                          ▼                          ▼
┌───────────────┐        ┌───────────────┐        ┌───────────────┐
│   Developer   │        │  CI/CD System │        │  Data Ops     │
│   Workstation │        │   (Optional)  │        │     Team      │
└───────┬───────┘        └───────┬───────┘        └───────┬───────┘
        │                        │                        │
        │                        │                        │
        └────────────────────────┼────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │   Deploy Script        │
                    │   ./deploy.sh          │
                    └────────┬───────────────┘
                             │
                             │ [Auto-Detect Pipeline Type]
                             │
            ┌────────────────┼────────────────┐
            │                                 │
            ▼                                 ▼
   ┌─────────────────┐              ┌─────────────────┐
   │ Streaming Gen   │              │  Batch SQL Gen  │
   │ generate_dab_   │              │  generate_dab   │
   │ streaming.py    │              │  .py            │
   └────────┬────────┘              └────────┬────────┘
            │                                 │
            └────────────────┬────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ databricks.yml  │
                    │  (Generated)    │
                    └────────┬────────┘
                             │
                             ▼
              ┌──────────────────────────┐
              │  Databricks Workspace    │
              │  ┌────────────────────┐  │
              │  │  Bundle Validate   │  │
              │  └──────────┬─────────┘  │
              │             │             │
              │             ▼             │
              │  ┌────────────────────┐  │
              │  │  Bundle Deploy     │  │
              │  └──────────┬─────────┘  │
              │             │             │
              │             ▼             │
              │  ┌────────────────────┐  │
              │  │  Jobs Created      │  │
              │  │  & Ready to Run    │  │
              │  └────────────────────┘  │
              └──────────────────────────┘
```

---

## 🔧 Component Architecture

### Framework Components

```
loyalty2.0_dab_framework/
│
├─── 📂 config/                    ← Environment Configuration Layer
│    ├── dev.yml                   (2 workers, 30min timeout)
│    ├── staging.yml               (3 workers, 60min timeout)
│    └── prod.yml                  (5 workers, monitoring enabled)
│
├─── 📂 pipelines/                 ← Data Pipeline Layer
│    ├── kafka_to_lakebase/        [Streaming → Postgres]
│    ├── gap_injection_with_kafka/ [Streaming → Delta Bronze]
│    ├── kafka_traffic_pipeline/   [Batch: Bronze → Silver → Gold]
│    └── template/                 [Template for new pipelines]
│
├─── 📂 src/utils/                 ← Code Generation Layer
│    ├── generate_dab.py           (Batch SQL pipelines)
│    ├── generate_dab_streaming.py (Streaming pipelines)
│    └── logger.py                 (Logging utility)
│
├─── 📂 scripts/                   ← Orchestration Layer
│    ├── deploy.sh                 (Universal deployment)
│    └── setup.sh                  (Initial setup)
│
├─── 📂 tests/                     ← Validation Layer
│    └── test_deployment.py        (10 tests: structure + generation)
│
└─── 📄 databricks.yml             ← Root Bundle Definition
```

---

## 📊 Data Flow Architecture

### Complete Data Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
├─────────────────────────────────────────────────────────────────────┤
│  🌊 Kafka Topics              📁 External Systems                   │
│  • jomin_johny_fe_tech...     • APIs                                │
│  • Real-time events           • Databases                           │
└───────────┬─────────────────────────────┬───────────────────────────┘
            │                             │
            │                             │
┌───────────▼─────────────────────────────▼───────────────────────────┐
│                   BRONZE LAYER (Raw Data)                           │
├─────────────────────────────────────────────────────────────────────┤
│  Catalog: loyalty_dev                                               │
│  Schema: kafka_bronze                                               │
│  Tables:                                                            │
│    • kafka_data_bronze                                              │
│      - Raw Kafka messages                                           │
│      - With metadata (topic, partition, offset, timestamp)          │
│                                                                     │
│  Pipeline: Kafka Ingestion to Bronze                                │
│    └─ Job: kafka_ingestion_to_bronze_pipeline                       │
└───────────┬─────────────────────────────────────────────────────────┘
            │
            │ [ETL Transformation]
            │
┌───────────▼─────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Cleaned Data)                        │
├─────────────────────────────────────────────────────────────────────┤
│  Catalog: loyalty_dev                                               │
│  Schema: silver_db                                                  │
│  Tables:                                                            │
│    • traffic_data_cleaned                                           │
│      - Cleaned & validated                                          │
│      - Data quality scores                                          │
│      - Composite keys                                               │
│    • customer_engagement_summary                                    │
│      - Aggregated metrics                                           │
│      - Customer segmentation                                        │
│                                                                     │
│  Pipeline: Traffic Data ETL Pipeline                                │
│    └─ Job: traffic_data_etl_pipeline                                │
│       ├─ Step 0: setup_databases                                    │
│       ├─ Step 1: traffic_data_clean                                 │
│       └─ Step 2: user_engagement_summary                            │
└───────────┬─────────────────────────────────────────────────────────┘
            │
            │ [Business Logic]
            │
┌───────────▼─────────────────────────────────────────────────────────┐
│                   GOLD LAYER (Business Data)                        │
├─────────────────────────────────────────────────────────────────────┤
│  Catalog: loyalty_dev                                               │
│  Schema: gold_db                                                    │
│  Tables:                                                            │
│    • traffic_data_enriched                                          │
│      - Business-ready data                                          │
│      - No technical metadata                                        │
│      - Optimized for analytics                                      │
│    • customer_locations                                             │
│      - Dimension table                                              │
│                                                                     │
│  Pipeline: Traffic Data ETL Pipeline                                │
│    └─ Job: traffic_data_etl_pipeline                                │
│       └─ Step 3: traffic_data_enriched                              │
└───────────┬─────────────────────────────────────────────────────────┘
            │
            │
┌───────────▼─────────────────────────────────────────────────────────┐
│          EXTERNAL TARGETS (Parallel Stream)                         │
├─────────────────────────────────────────────────────────────────────┤
│  🐘 PostgreSQL (Lakebase)                                           │
│    • Instance: jomin-gap-demo                                       │
│    • Database: databricks_postgres                                  │
│    • Schema: gold_db                                                │
│    • Table: kafka_data                                              │
│                                                                     │
│  Pipeline: Kafka to Lakebase Streaming                              │
│    └─ Job: kafka_to_lakebase_streaming_pipeline                     │
│       ├─ Task 1: kafka_to_postgres_ingestion                        │
│       └─ Task 2: customer_stats_generator                           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Deployment Architecture

### Deployment Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT WORKFLOW                           │
└──────────────────────────────────────────────────────────────────┘

   Developer Action
        │
        ├─ ./deploy.sh <pipeline_name> <env> <profile>
        │
        ▼
   ┌────────────────────────────────────────────┐
   │  STEP 1: Pipeline Type Detection           │
   ├────────────────────────────────────────────┤
   │  • Read config.json                        │
   │  • Check for 'tasks' → Streaming           │
   │  • Check for 'execution_sequence' → Batch  │
   │  • Auto-detect and proceed                 │
   └────────────────┬───────────────────────────┘
                    │
                    ▼
   ┌────────────────────────────────────────────┐
   │  STEP 2: Generate databricks.yml           │
   ├────────────────────────────────────────────┤
   │  IF Streaming:                             │
   │    → generate_dab_streaming.py             │
   │    → Parse tasks, build job config         │
   │    → Set timeout=0 (continuous)            │
   │                                            │
   │  IF Batch:                                 │
   │    → generate_dab.py                       │
   │    → Parse execution_sequence              │
   │    → Build task dependencies               │
   │    → Set timeout>0 (batch)                 │
   │                                            │
   │  OUTPUT: databricks.yml                    │
   └────────────────┬───────────────────────────┘
                    │
                    ▼
   ┌────────────────────────────────────────────┐
   │  STEP 3: Validate Bundle                   │
   ├────────────────────────────────────────────┤
   │  • databricks bundle validate              │
   │  • Check YAML syntax                       │
   │  • Verify workspace connection             │
   │  • Validate resource definitions           │
   └────────────────┬───────────────────────────┘
                    │
                    ▼
   ┌────────────────────────────────────────────┐
   │  STEP 4: Deploy to Databricks              │
   ├────────────────────────────────────────────┤
   │  • Upload files to workspace               │
   │  • Create/update jobs                      │
   │  • Configure clusters                      │
   │  • Set up notifications                    │
   └────────────────┬───────────────────────────┘
                    │
                    ▼
   ┌────────────────────────────────────────────┐
   │  STEP 5: Deployment Summary                │
   ├────────────────────────────────────────────┤
   │  • Show job URL                            │
   │  • Display job configuration               │
   │  • Provide run command                     │
   └────────────────────────────────────────────┘
                    │
                    ▼
              ✅ COMPLETE
         Job ready in Databricks!
```

---

## 🔄 Pipeline Types

### 1. Streaming Pipelines

```
┌─────────────────────────────────────────────────────────┐
│             STREAMING PIPELINE ARCHITECTURE             │
└─────────────────────────────────────────────────────────┘

Config Marker: "tasks" key present

  Kafka Topic
      │
      ├─ Read Stream (Continuous)
      │
      ▼
  ┌─────────────────┐
  │  Task 1:        │
  │  Ingestion      │◄─── Shared Job Cluster
  └────────┬────────┘     (Cost Efficient)
           │
           │ [Parallel]
           │
           ▼
  ┌─────────────────┐
  │  Task 2:        │◄─── Same Cluster
  │  Processing     │
  └────────┬────────┘
           │
           ▼
    Target System
    (Delta/Postgres)

Characteristics:
• timeout_seconds: 0 (continuous)
• Shared cluster for all tasks
• Real-time processing
• Checkpoint-based recovery

Examples:
• Kafka to Lakebase Streaming
• Kafka Ingestion to Bronze
```

### 2. Batch SQL Pipelines

```
┌─────────────────────────────────────────────────────────┐
│              BATCH SQL PIPELINE ARCHITECTURE            │
└─────────────────────────────────────────────────────────┘

Config Marker: "execution_sequence" key present

  Step 0: Setup
      │
      └─ Create databases & schemas
           │
           ▼
  ┌────────────────────┐
  │  Bronze → Silver   │
  ├────────────────────┤
  │  Step 1 & Step 2   │◄─── Serverless/Dedicated
  │  (Parallel)        │     Compute
  └──────────┬─────────┘
             │
             │ [depends_on]
             │
             ▼
  ┌────────────────────┐
  │  Silver → Gold     │
  ├────────────────────┤
  │  Step 3            │◄─── Task Dependencies
  └──────────┬─────────┘
             │
             ▼
      Gold Layer Tables

Characteristics:
• timeout_seconds: 10800 (3 hours)
• Task dependencies (DAG)
• Scheduled execution
• Bronze → Silver → Gold

Examples:
• Traffic Data ETL Pipeline
```

---

## ⚙️ Configuration Architecture

### Configuration Hierarchy

```
┌─────────────────────────────────────────────────────────┐
│              CONFIGURATION ARCHITECTURE                 │
└─────────────────────────────────────────────────────────┘

ROOT: databricks.yml
│
├─ Bundle Metadata
│  └─ name: loyalty2.0_framework
│
├─ Global Variables
│  ├─ catalog (default: loyalty_dev)
│  └─ notification_email
│
├─ Environment Targets
│  ├─ dev/
│  │  ├─ mode: development
│  │  ├─ workspace: {...}
│  │  └─ variables: {...}
│  │
│  ├─ staging/
│  │  ├─ mode: development
│  │  └─ variables: {...}
│  │
│  └─ prod/
│     ├─ mode: production
│     └─ variables: {...}
│
└─ Includes
   └─ pipelines/*/databricks.yml

───────────────────────────────────────────

PIPELINE LEVEL: pipelines/<name>/config.json
│
├─ Pipeline Identity
│  ├─ pipeline_name: "Kafka to Lakebase Streaming"
│  ├─ pipeline_type: "streaming_to_postgres"
│  └─ description: "..."
│
├─ Pipeline Tasks/Steps
│  ├─ tasks: [...]              (for streaming)
│  └─ execution_sequence: [...]  (for batch)
│
├─ Resource Configuration
│  ├─ job_cluster: {...}
│  ├─ kafka_source: {...}
│  └─ postgres_connection: {...}
│
└─ Settings
   ├─ notification_email
   └─ max_concurrent_runs

───────────────────────────────────────────

ENVIRONMENT: config/<env>.yml
│
├─ Environment Identity
│  └─ environment: dev/staging/prod
│
├─ Databricks Config
│  ├─ host
│  └─ workspace_path
│
├─ Catalog Config
│  ├─ name
│  └─ schemas: [bronze, silver, gold]
│
├─ Cluster Defaults
│  ├─ spark_version
│  ├─ node_type_id
│  ├─ num_workers
│  └─ autotermination_minutes
│
└─ Pipeline Defaults
   ├─ timeout_seconds
   ├─ checkpoint_location
   └─ max_concurrent_runs
```

---

## 🔒 Security Architecture

### Security Layers

```
┌─────────────────────────────────────────────────────────┐
│                  SECURITY ARCHITECTURE                  │
└─────────────────────────────────────────────────────────┘

Layer 1: Authentication
├─ Databricks CLI
│  ├─ Profile-based auth (--profile jomin)
│  ├─ Token-based authentication
│  └─ OAuth support (future)
│
└─ Workspace Access
   ├─ User: jomin.johny@databricks.com
   └─ Permissions: CAN_MANAGE

Layer 2: Authorization
├─ Unity Catalog
│  ├─ Catalog-level permissions
│  ├─ Schema-level access control
│  └─ Table-level grants
│
└─ Job Permissions
   ├─ Group: loyalty-viewers (CAN_VIEW)
   └─ Group: loyalty-operators (CAN_MANAGE_RUN)

Layer 3: Data Security
├─ Secrets Management
│  ├─ Secret Scopes
│  │  └─ oetrta (Kafka credentials)
│  ├─ Secret Keys
│  │  └─ kafka-bootstrap-servers-plaintext
│  └─ No hardcoded credentials
│
└─ Data Isolation
   ├─ Dev: loyalty_dev catalog
   ├─ Staging: loyalty_staging catalog
   └─ Prod: loyalty_prod catalog

Layer 4: Compute Security
├─ Cluster Security Mode
│  └─ USER_ISOLATION (enforced)
│
├─ Network Security
│  └─ Private subnet communication
│
└─ Data at Rest
   └─ Encrypted Delta tables

Layer 5: Audit & Monitoring
├─ Job Notifications
│  ├─ on_failure: email alerts
│  └─ on_success: prod only
│
├─ Workspace Logs
│  └─ All deployment actions logged
│
└─ Data Lineage
   └─ Unity Catalog tracking
```

---

## 📈 Naming Convention

### Job Naming Pattern

```
Job Name Format:
[Loyalty2.0] [${bundle.target}] <Pipeline Display Name>

Examples:
┌──────────────────────────────────────────────────────┐
│ Environment │ Pipeline Name                          │
├──────────────────────────────────────────────────────┤
│ dev         │ [Loyalty2.0] [dev] Kafka to Lakebase  │
│             │ Streaming                              │
├──────────────────────────────────────────────────────┤
│ staging     │ [Loyalty2.0] [staging] Traffic Data   │
│             │ ETL Pipeline                           │
├──────────────────────────────────────────────────────┤
│ prod        │ [Loyalty2.0] [prod] Kafka Ingestion   │
│             │ to Bronze                              │
└──────────────────────────────────────────────────────┘

Benefits:
✅ Brand visibility ([Loyalty2.0])
✅ Environment clarity ([dev/staging/prod])
✅ Human-readable names (Kafka to Lakebase Streaming)
```

---

## 🔄 End-to-End Workflow

### Complete Pipeline Lifecycle

```
┌─────────────────────────────────────────────────────────┐
│            PIPELINE LIFECYCLE WORKFLOW                  │
└─────────────────────────────────────────────────────────┘

1. DEVELOPMENT
   │
   ├─ Create pipeline from template
   │  └─ cp -r pipelines/template pipelines/my_pipeline
   │
   ├─ Configure pipeline
   │  └─ Edit config.json (pipeline_name, tasks, settings)
   │
   └─ Add notebooks
      └─ Write transformation logic

2. TESTING
   │
   ├─ Run unit tests
   │  └─ python3 tests/test_deployment.py
   │
   ├─ Generate bundle locally
   │  └─ python3 src/utils/generate_dab_streaming.py my_pipeline
   │
   └─ Validate YAML
      └─ databricks bundle validate -t dev

3. DEPLOYMENT (DEV)
   │
   ├─ Deploy to dev
   │  └─ ./scripts/deploy.sh my_pipeline dev jomin
   │
   ├─ Run pipeline
   │  └─ databricks bundle run <job> -t dev --profile jomin
   │
   └─ Verify results
      └─ Check tables, logs, metrics

4. PROMOTION (STAGING)
   │
   ├─ Deploy to staging
   │  └─ ./scripts/deploy.sh my_pipeline staging jomin
   │
   ├─ Run integration tests
   │  └─ Verify with real data volumes
   │
   └─ Performance validation
      └─ Check cluster usage, execution time

5. PRODUCTION (PROD)
   │
   ├─ Deploy to prod
   │  └─ ./scripts/deploy.sh my_pipeline prod jomin
   │
   ├─ Schedule job
   │  └─ Configure cron expression
   │
   ├─ Monitor
   │  └─ Email notifications, dashboards
   │
   └─ Maintain
      └─ Update notebooks, redeploy as needed
```

---

## 📊 Metrics & Monitoring

### Key Metrics

```
Pipeline Health Metrics:
├─ Execution Time
│  ├─ Avg: < 30 minutes (batch)
│  └─ Continuous (streaming)
│
├─ Success Rate
│  └─ Target: > 99%
│
├─ Data Quality
│  ├─ Null percentage
│  ├─ Duplicate count
│  └─ Schema compliance
│
└─ Resource Utilization
   ├─ Cluster efficiency
   └─ Cost per pipeline run

Framework Metrics:
├─ Deployment Success Rate
│  └─ Target: 100%
│
├─ Validation Pass Rate
│  └─ 10/10 tests passing
│
└─ Time to Deploy
   └─ Avg: < 2 minutes
```

---

## 🎯 Summary

### Framework Capabilities

| Capability | Status | Details |
|-----------|--------|---------|
| **Multi-Pipeline** | ✅ Production | 3 pipelines deployed |
| **Auto-Detection** | ✅ Production | Streaming & Batch SQL |
| **Multi-Environment** | ✅ Production | Dev, Staging, Prod |
| **Clean Naming** | ✅ Production | `[Loyalty2.0]` branded |
| **Testing** | ✅ Production | 10/10 tests passing |
| **Documentation** | ✅ Complete | This document + guides |

### Deployment Stats

- **Total Pipelines**: 3
- **Total Jobs**: 3 (one per pipeline)
- **Total Tasks**: 7 (across all pipelines)
- **Lines of Code**: 2,559
- **Python Files**: 17
- **Tests Passing**: 10/10 (100%)

---

## 📞 Support

For questions or issues:
- **Documentation**: README.md, DEPLOYMENT_GUIDE.md
- **Tests**: `python3 tests/test_deployment.py`
- **Contact**: data-team@company.com

---

**Built with ❤️ following Databricks & Microsoft best practices**

*End of Architecture Document*

