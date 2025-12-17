#!/usr/bin/env python3
"""
Unified DAB Generator for Kafka Streaming Pipelines
Supports both Kafka to Lakehouse (Delta) and Kafka to Lakebase (PostgreSQL)
Usage: python generate_dab_streaming.py <pipeline_name>
"""

import yaml
import json
import os
import sys
import shutil
from pathlib import Path

def read_pipeline_config(pipeline_name):
    """Read the streaming pipeline configuration"""
    config_file = f'pipelines/{pipeline_name}/config/config.json'
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Pipeline config not found: {config_file}")
        return None

def get_source_config(pipeline_config, target_env='dev'):
    """
    Determine notebook source configuration based on pipeline config and environment
    
    Returns dict with 'source_type' and optional 'git_config'
    """
    source_config = pipeline_config.get('source_config', {})
    
    # Get environment-specific override, fallback to default
    env_overrides = source_config.get('environment_overrides', {})
    source_type = env_overrides.get(target_env, source_config.get('default_source', 'WORKSPACE'))
    
    # Branch mapping for Git: dev->dev, staging->staging, prod->main
    git_branch_map = {
        'dev': 'dev',
        'staging': 'staging', 
        'prod': 'main'
    }
    
    result = {
        'source_type': source_type
    }
    
    if source_type == 'GIT':
        git_config = source_config.get('git_config', {})
        result['git_config'] = {
            'git_url': git_config.get('git_url', ''),
            'git_branch': git_branch_map.get(target_env, 'main'),
            'git_provider': git_config.get('git_provider', 'gitHubEnterprise')
        }
    
    return result

def build_notebook_task_config(notebook_path, base_parameters, source_config):
    """
    Build notebook_task configuration with appropriate source (WORKSPACE or GIT)
    
    Args:
        notebook_path: Path to notebook
        base_parameters: Base parameters dict
        source_config: Source configuration from get_source_config()
    
    Returns:
        notebook_task dict
    """
    task_config = {
        'notebook_path': notebook_path,
        'source': source_config['source_type']
    }
    
    if base_parameters:
        task_config['base_parameters'] = base_parameters
    
    # Add git_source block if using GIT
    if source_config['source_type'] == 'GIT':
        git_cfg = source_config['git_config']
        task_config['git_source'] = {
            'git_url': git_cfg['git_url'],
            'git_branch': git_cfg['git_branch'],
            'git_provider': git_cfg['git_provider']
        }
    
    return task_config

def generate_lakebase_task(pipeline_config, source_config):
    """Generate tasks for Kafka to Lakebase/Postgres pipeline
    
    Args:
        pipeline_config: Pipeline configuration dict
        source_config: Source configuration from get_source_config()
    """
    
    kafka_source = pipeline_config.get('kafka_source', {})
    
    # Support both lakebase_connection and postgres_connection
    lakebase_conn = pipeline_config.get('lakebase_connection', {})
    postgres_conn = pipeline_config.get('postgres_connection', {})
    db_conn = lakebase_conn or postgres_conn
    
    # Support both target formats
    target = pipeline_config.get('target', {})
    streaming_config = pipeline_config.get('streaming_config', {})
    settings = pipeline_config.get('settings', {})
    
    # Check for injection_sources (for gap_injection_with_kafka type)
    injection_sources = pipeline_config.get('injection_sources', [])
    
    # Get table info from either format
    if target:
        table_schema = target.get('lakebase_schema', 'public')
        table_name = target.get('lakebase_table', 'N/A')
    else:
        table_schema = postgres_conn.get('postgres_schema', 'public')
        table_name = postgres_conn.get('postgres_table', 'N/A')
    
    print(f"\n📊 Postgres/Lakebase Target:")
    print(f"  - Kafka Topic: {kafka_source.get('kafka_topic', 'N/A')}")
    print(f"  - Instance: {db_conn.get('lakebase_instance_name', db_conn.get('postgres_instance_name', 'N/A'))}")
    print(f"  - Table: {table_schema}.{table_name}")
    print(f"  - Trigger: {streaming_config.get('trigger_mode', 'processingTime')} - {streaming_config.get('trigger_interval', '10 seconds')}")
    
    # Check if tasks are defined in config
    tasks_config = pipeline_config.get('tasks', [])
    job_cluster_config = pipeline_config.get('job_cluster', None)
    
    if tasks_config:
        # Generate tasks from config
        print(f"\n📋 Generating {len(tasks_config)} tasks from config:")
        
        if job_cluster_config:
            job_cluster_key = job_cluster_config.get('job_cluster_key', 'shared_cluster')
            num_workers = job_cluster_config.get('num_workers', 2)
            print(f"  🔗 Using shared job cluster: {job_cluster_key} ({num_workers} workers)")
        
        all_tasks = []
        
        for task_config in tasks_config:
            task_key = task_config.get('task_key')
            notebook_path = task_config.get('notebook_path')
            description = task_config.get('description', '')
            
            # Build base parameters
            base_params = {}
            
            # If injection_sources exist, pass individual parameters from first source
            if injection_sources:
                source = injection_sources[0]
                kafka_cfg = source.get('kafka_config', {})
                base_params = {
                    'kafka_topic': kafka_cfg.get('kafka_topic', ''),
                    'kafka_bootstrap_servers_secret_scope': kafka_cfg.get('kafka_bootstrap_servers_secret_scope', ''),
                    'kafka_bootstrap_servers_secret_key': kafka_cfg.get('kafka_bootstrap_servers_secret_key', ''),
                    'kafka_security_protocol': kafka_cfg.get('kafka_security_protocol', 'PLAINTEXT'),
                    'starting_offsets': kafka_cfg.get('starting_offsets', 'earliest'),
                    'target_catalog': source.get('target_catalog', ''),
                    'target_schema': source.get('target_schema', ''),
                    'target_table': source.get('target_table', ''),
                    'checkpoint_location': source.get('checkpoint_location', ''),
                    'merge_schema': str(source.get('merge_schema', True)).lower()
                }
            else:
                # Default to config_path for other types
                base_params = {
                    'config_path': 'config/config.json'
                }
            
            # Build notebook task config with appropriate source
            notebook_task = build_notebook_task_config(notebook_path, base_params, source_config)
            
            task = {
                'task_key': task_key,
                'notebook_task': notebook_task
            }
            
            # Use job cluster if defined, otherwise create new cluster per task
            if job_cluster_config:
                task['job_cluster_key'] = job_cluster_config.get('job_cluster_key', 'shared_cluster')
            else:
                num_workers = task_config.get('num_workers', 2)
                task['new_cluster'] = {
                    'spark_version': '15.4.x-scala2.12',
                    'node_type_id': 'Standard_DS3_v2',
                    'num_workers': num_workers,
                    'data_security_mode': 'USER_ISOLATION',
                    'spark_conf': {
                        'spark.jars.packages': 'org.postgresql:postgresql:42.7.1'
                    }
                }
            
            all_tasks.append(task)
            print(f"  ✓ Task: {task_key}")
            print(f"    Notebook: {notebook_path}")
            print(f"    Description: {description}")
        
        return all_tasks, job_cluster_config
    
    else:
        # Legacy: single task for backward compatibility
        print(f"  ✓ Task: kafka_to_postgres_ingestion (legacy mode)")
        
        # Build notebook task config with appropriate source
        notebook_task = build_notebook_task_config(
            'notebooks/kafka_to_postgres.py',
            {'config_path': 'config/config.json'},
            source_config
        )
        
        task = {
            'task_key': 'kafka_to_postgres_ingestion',
            'new_cluster': {
                'spark_version': '15.4.x-scala2.12',
                'node_type_id': 'Standard_DS3_v2',
                'num_workers': 2,
                'data_security_mode': 'USER_ISOLATION',
                'spark_conf': {
                    'spark.jars.packages': 'org.postgresql:postgresql:42.7.1'
                }
            },
            'notebook_task': notebook_task
        }
        return [task], None

def generate_databricks_yml(pipeline_name):
    """Generate databricks.yml for streaming pipeline"""
    
    print(f"\n{'='*80}")
    print(f"GENERATING DATABRICKS.YML FOR STREAMING PIPELINE: {pipeline_name}")
    print(f"{'='*80}\n")
    
    # Read pipeline config
    pipeline_config = read_pipeline_config(pipeline_name)
    
    if not pipeline_config:
        print(f"❌ Cannot generate databricks.yml for {pipeline_name}")
        return False
    
    # Get target environment (from env var or default to 'dev')
    target_env = os.environ.get('TARGET_ENV', 'dev')
    
    # Determine source configuration based on environment
    source_config = get_source_config(pipeline_config, target_env)
    
    display_name = pipeline_config.get('pipeline_name', pipeline_name)
    # Sanitize job key: remove brackets and special chars, replace spaces/dashes with underscores
    import re
    job_key_base = re.sub(r'[^\w\s-]', '', display_name).lower().replace(' ', '_').replace('-', '_')
    pipeline_type = pipeline_config.get('pipeline_type', 'streaming')
    
    print(f"📌 Display Name: {display_name}")
    print(f"📌 Job Key: {job_key_base}")
    print(f"📌 Type: {pipeline_type}")
    print(f"📌 Target Environment: {target_env}")
    print(f"📌 Notebook Source: {source_config['source_type']}")
    if source_config['source_type'] == 'GIT':
        git_cfg = source_config['git_config']
        print(f"   ├─ Git URL: {git_cfg['git_url']}")
        print(f"   ├─ Git Branch: {git_cfg['git_branch']}")
        print(f"   └─ Git Provider: {git_cfg['git_provider']}")
    
    # Generate tasks for streaming to postgres/lakebase
    print(f"\n🔵 Detected: Kafka to Postgres/Lakebase pipeline")
    all_tasks, job_cluster_config = generate_lakebase_task(pipeline_config, source_config)
    job_suffix = '_pipeline'
    timeout = 0  # No timeout for continuous streaming
    
    # Get workspace configuration from config
    workspace_config = pipeline_config.get('workspace_config', {})
    dev_workspace = workspace_config.get('dev', 'https://e2-demo-field-eng.cloud.databricks.com')
    staging_workspace = workspace_config.get('staging', dev_workspace)
    prod_workspace = workspace_config.get('prod', dev_workspace)
    
    # Base DAB configuration
    dab_config = {
        'bundle': {
            'name': f'{pipeline_name}_bundle'
        },
        'sync': {
            'include': [
                './**'
            ]
        },
        'variables': {
            'catalog': {
                'description': 'Unity Catalog name',
                'default': pipeline_config.get('catalog')
            }
        },
        'targets': {
            'dev': {
                'mode': 'development',
                'workspace': {
                    'host': dev_workspace
                },
                'variables': {
                    'catalog': pipeline_config.get('catalog')
                }
            },
            'staging': {
                'mode': 'development',
                'workspace': {
                    'host': staging_workspace
                },
                'variables': {
                    'catalog': pipeline_config.get('catalog')
                }
            },
            'prod': {
                'mode': 'production',
                'workspace': {
                    'host': prod_workspace
                },
                'variables': {
                    'catalog': pipeline_config.get('catalog')
                }
            }
        },
        'resources': {
            'jobs': {}
        }
    }
    
    # Get notification email
    notification_email = pipeline_config.get('settings', {}).get('notification_email', 'data-team@company.com')
    
    # Create the job
    job_key = f'{job_key_base}{job_suffix}'
    job_config = {
        'name': display_name,
        'tasks': all_tasks,
        'timeout_seconds': timeout,
        'max_concurrent_runs': 1,
        'email_notifications': {
            'on_failure': [notification_email]
        }
    }
    
    # Add job cluster if defined
    if job_cluster_config:
        job_config['job_clusters'] = [{
            'job_cluster_key': job_cluster_config.get('job_cluster_key', 'shared_cluster'),
            'new_cluster': {
                'spark_version': job_cluster_config.get('spark_version', '15.4.x-scala2.12'),
                'node_type_id': job_cluster_config.get('node_type_id', 'Standard_DS3_v2'),
                'num_workers': job_cluster_config.get('num_workers', 2),
                'data_security_mode': job_cluster_config.get('data_security_mode', 'USER_ISOLATION'),
                'spark_conf': {
                    'spark.jars.packages': 'org.postgresql:postgresql:42.7.1'
                }
            }
        }]
    
    dab_config['resources']['jobs'][job_key] = job_config
    
    # Write to pipeline directory
    output_file = f'pipelines/{pipeline_name}/databricks.yml'
    with open(output_file, 'w') as f:
        yaml.dump(dab_config, f, default_flow_style=False, sort_keys=False, indent=2)
    
    print(f"\n✅ Generated {output_file}")
    print(f"   Job Name: {job_key}")
    print(f"   Total Tasks: {len(all_tasks)}")
    print(f"   Timeout: {'Continuous (no timeout)' if timeout == 0 else f'{timeout} seconds'}")
    print(f"\n{'='*80}\n")
    
    return True

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("=" * 80)
        print("Unified DAB Generator for Kafka Streaming Pipelines")
        print("=" * 80)
        print("\nUsage: python generate_dab_streaming.py <pipeline_name>")
        print("\nExamples:")
        print("  python generate_dab_streaming.py kafka_to_lakebase")
        print("\nThe pipeline type is auto-detected from config.json")
        print("=" * 80)
        sys.exit(1)
    
    pipeline_name = sys.argv[1]
    success = generate_databricks_yml(pipeline_name)
    
    if success:
        print(f"✅ Done! Streaming DAB configuration created for {pipeline_name}")
        print("\n📋 Next steps:")
        print(f"  1. Review config: pipelines/{pipeline_name}/config/config.json")
        print(f"  2. Validate: cd pipelines/{pipeline_name} && databricks bundle validate -t dev")
        print(f"  3. Deploy: databricks bundle deploy -t dev")
        print(f"  4. Run: databricks bundle run <job_name> -t dev")
    else:
        print(f"❌ Failed to generate DAB configuration for {pipeline_name}")
        sys.exit(1)

