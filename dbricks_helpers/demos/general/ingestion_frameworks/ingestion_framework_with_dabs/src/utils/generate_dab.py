#!/usr/bin/env python3
"""
Generate databricks.yml from pipeline config
Usage: python generate_dab.py <pipeline_name>
"""

import yaml
import json
import os
import sys
from pathlib import Path

def read_pipeline_config(pipeline_name):
    """Read the pipeline configuration"""
    config_file = f'pipelines/{pipeline_name}/config.json'
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
        tuple: (notebook_task dict, git_source dict or None)
    """
    task_config = {
        'notebook_path': notebook_path,
        'source': source_config['source_type']
    }
    
    if base_parameters:
        task_config['base_parameters'] = base_parameters
    
    # Return git_source separately to be added at task level
    git_source = None
    if source_config['source_type'] == 'GIT':
        git_cfg = source_config['git_config']
        git_source = {
            'git_url': git_cfg['git_url'],
            'git_branch': git_cfg['git_branch'],
            'git_provider': git_cfg['git_provider']
        }
    
    return task_config, git_source

def generate_databricks_yml(pipeline_name):
    """Generate databricks.yml for a specific pipeline"""
    
    print(f"\n{'='*80}")
    print(f"GENERATING DATABRICKS.YML FOR: {pipeline_name}")
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
    job_name = re.sub(r'[^\w\s-]', '', display_name).lower().replace(' ', '_').replace('-', '_')
    execution_sequence = pipeline_config.get('execution_sequence', [])
    
    print(f"Display Name: {display_name}")
    print(f"Job Key: {job_name}")
    print(f"Total steps: {len(execution_sequence)}")
    print(f"Target Environment: {target_env}")
    print(f"Notebook Source: {source_config['source_type']}")
    if source_config['source_type'] == 'GIT':
        git_cfg = source_config['git_config']
        print(f"   ├─ Git URL: {git_cfg['git_url']}")
        print(f"   ├─ Git Branch: {git_cfg['git_branch']}")
        print(f"   └─ Git Provider: {git_cfg['git_provider']}")
    
    # Group by layer
    setup_steps = [s for s in execution_sequence if s.get('layer') == 'setup']
    bronze_to_silver_steps = [s for s in execution_sequence if s.get('layer') == 'bronze_to_silver']
    silver_to_gold_steps = [s for s in execution_sequence if s.get('layer') == 'silver_to_gold']
    
    if setup_steps:
        print(f"  Setup: {len(setup_steps)} step(s)")
    print(f"  Bronze to Silver: {len(bronze_to_silver_steps)} step(s)")
    for step in bronze_to_silver_steps:
        print(f"    - Step {step['step_id']}: {step['step_name']}")
    
    print(f"  Silver to Gold: {len(silver_to_gold_steps)} step(s)")
    for step in silver_to_gold_steps:
        print(f"    - Step {step['step_id']}: {step['step_name']} (depends on: {step.get('depends_on', [])})")
    print()
    
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
                f'pipelines/{pipeline_name}/**'
            ]
        },
        'variables': {
            'catalog': {
                'description': 'Unity Catalog name',
                'default': pipeline_config.get('catalog')
            },
            'bronze_database': {
                'description': 'Bronze layer database',
                'default': pipeline_config.get('databases', {}).get('bronze')
            },
            'silver_database': {
                'description': 'Silver layer database',
                'default': pipeline_config.get('databases', {}).get('silver')
            },
            'gold_database': {
                'description': 'Gold layer database',
                'default': pipeline_config.get('databases', {}).get('gold')
            },
            'metadata_database': {
                'description': 'Framework metadata database',
                'default': pipeline_config.get('databases', {}).get('metadata')
            },
            'force_recreate': {
                'description': 'Force recreate tables even if they exist',
                'default': pipeline_config.get('settings', {}).get('force_recreate', 'false')
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
    
    # Build tasks from execution sequence
    all_tasks = []
    step_id_to_task_key = {}
    
    for step in execution_sequence:
        step_id = step['step_id']
        step_name = step['step_name']
        task_key = f"step_{step_id}_{step_name}"
        
        step_id_to_task_key[step_id] = task_key
        
        # Get the notebook path for this step
        notebook_path = step.get('notebook_path', '')
        
        # Build base parameters - Generic approach: pass all common variables
        base_params = {
            'catalog': '${var.catalog}',
            'bronze_database': '${var.bronze_database}',
            'silver_database': '${var.silver_database}',
            'gold_database': '${var.gold_database}',
            'metadata_database': '${var.metadata_database}',
            'force_recreate': '${var.force_recreate}'
        }
        
        # Generic: Add source info if present in step config
        if 'source_catalog' in step:
            base_params['source_catalog'] = step.get('source_catalog', '${var.catalog}')
        if 'source_database' in step:
            base_params['source_database'] = step['source_database']
        if 'source_table' in step:
            base_params['source_table'] = step['source_table']
        
        # Generic: Add target info if present in step config
        if 'target_catalog' in step:
            base_params['target_catalog'] = step.get('target_catalog', '${var.catalog}')
        if 'target_database' in step:
            base_params['target_database'] = step['target_database']
        if 'target_table' in step:
            base_params['target_table'] = step['target_table']
            base_params['table_name'] = step['target_table']
            # Also add as bronze_table for backwards compatibility
            base_params['bronze_table'] = step['target_table']
        
        # Generic: Add mode
        if 'mode' in step:
            base_params['mode'] = step['mode']
        
        # Generic: Add any custom parameters from step config
        if 'parameters' in step:
            base_params.update(step.get('parameters', {}))
        
        # Generic: Build task based on transformation_type
        transformation_type = step.get('transformation_type', 'notebook')
        
        task = {
            'task_key': task_key
        }
        
        # Support different task types: notebook, python, sql, dbt, etc.
        if transformation_type == 'notebook':
            notebook_task, git_source = build_notebook_task_config(notebook_path, base_params, source_config)
            task['notebook_task'] = notebook_task
            # Add git_source at task level, not inside notebook_task
            if git_source:
                task['git_source'] = git_source
        elif transformation_type == 'python':
            # Python task support
            task['python_task'] = {
                'python_file': step.get('python_file', notebook_path),
                'source': source_config['source_type']
            }
            if base_params:
                task['python_task']['parameters'] = [f"--{k}={v}" for k, v in base_params.items()]
        elif transformation_type == 'sql':
            # SQL task support
            task['sql_task'] = {
                'file': {
                    'path': step.get('sql_file', notebook_path)
                },
                'warehouse_id': step.get('warehouse_id', '${var.warehouse_id}')
            }
        elif transformation_type == 'dbt':
            # DBT task support
            task['dbt_task'] = {
                'project_directory': step.get('dbt_project_directory', ''),
                'commands': step.get('dbt_commands', ['dbt run'])
            }
        else:
            # Default to notebook for backward compatibility
            notebook_task = build_notebook_task_config(notebook_path, base_params, source_config)
            task['notebook_task'] = notebook_task
        
        # Add dependencies
        depends_on = step.get('depends_on', [])
        if depends_on:
            task['depends_on'] = [
                {'task_key': step_id_to_task_key[dep_id]} 
                for dep_id in depends_on 
                if dep_id in step_id_to_task_key
            ]
        
        all_tasks.append(task)
    
    # Create the job
    dab_config['resources']['jobs'][job_name] = {
        'name': display_name,
        'tasks': all_tasks,
        'timeout_seconds': 10800,
        'max_concurrent_runs': 1,
        'job_clusters': [],
        'email_notifications': {
            'on_failure': [pipeline_config.get('settings', {}).get('notification_email', 'data-team@company.com')]
        }
    }
    
    # Write to pipeline directory
    output_file = f'pipelines/{pipeline_name}/databricks.yml'
    with open(output_file, 'w') as f:
        yaml.dump(dab_config, f, default_flow_style=False, sort_keys=False, indent=2)
    
    total_tasks = len(execution_sequence)
    
    print(f"✅ Generated {output_file}")
    print(f"   Job: {job_name}")
    print(f"   Total tasks: {total_tasks}")
    print(f"     - Setup: {len(setup_steps)} task(s)")
    print(f"     - Bronze to Silver: {len(bronze_to_silver_steps)} task(s)")
    print(f"     - Silver to Gold: {len(silver_to_gold_steps)} task(s)")
    print(f"\n{'='*80}\n")
    
    return True

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python generate_dab.py <pipeline_name>")
        print("\nExample: python generate_dab.py my_pipeline")
        sys.exit(1)
    
    pipeline_name = sys.argv[1]
    success = generate_databricks_yml(pipeline_name)
    
    if success:
        print(f"✅ Done! DAB configuration created for {pipeline_name}")
    else:
        print(f"❌ Failed to generate DAB configuration for {pipeline_name}")
        sys.exit(1)

