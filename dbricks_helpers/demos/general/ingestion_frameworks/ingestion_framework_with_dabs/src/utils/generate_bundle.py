"""Generate databricks.yml from pipeline config"""

import json
import sys
import yaml
from pathlib import Path
from typing import Dict, Any
from logger import get_logger

logger = get_logger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load pipeline configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            if config_path.endswith('.json'):
                return json.load(f)
            else:
                return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


def generate_databricks_yml(pipeline_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate databricks.yml structure from pipeline config.
    
    Args:
        pipeline_name: Name of the pipeline
        config: Pipeline configuration dictionary
    
    Returns:
        Databricks bundle configuration
    """
    logger.info(f"Generating databricks.yml for pipeline: {pipeline_name}")
    
    # Base configuration
    dab = {
        'bundle': {
            'name': f'{pipeline_name}_bundle'
        },
        'sync': {
            'include': ['./**']
        },
        'variables': {
            'catalog': {
                'description': 'Unity Catalog name',
                'default': config.get('catalog', {}).get('name', 'loyalty_dev')
            }
        },
        'resources': {
            'jobs': {}
        }
    }
    
    # Build tasks
    tasks = []
    for task_config in config.get('tasks', []):
        task = {
            'task_key': task_config['task_key'],
            'notebook_task': {
                'notebook_path': task_config['notebook_path'],
                'source': 'WORKSPACE'
            }
        }
        
        # Add cluster configuration
        if 'cluster' in config:
            cluster_cfg = config['cluster']
            task['new_cluster'] = {
                'spark_version': cluster_cfg.get('spark_version', '15.4.x-scala2.12'),
                'node_type_id': cluster_cfg.get('node_type_id', 'i3.xlarge'),
                'num_workers': cluster_cfg.get('num_workers', 2),
                'data_security_mode': cluster_cfg.get('data_security_mode', 'USER_ISOLATION')
            }
        
        # Add dependencies
        if 'depends_on' in task_config:
            task['depends_on'] = [
                {'task_key': dep} for dep in task_config['depends_on']
            ]
        
        tasks.append(task)
    
    # Build job
    job_name = config.get('pipeline_name', pipeline_name)
    description = config.get('description', f'{pipeline_name} pipeline')
    
    job = {
        'name': f'[${{{bundle.target}}}] {description}',
        'tasks': tasks,
        'timeout_seconds': config.get('pipeline_defaults', {}).get('timeout_seconds', 3600),
        'max_concurrent_runs': config.get('pipeline_defaults', {}).get('max_concurrent_runs', 1)
    }
    
    # Add notifications
    if 'notifications' in config:
        job['email_notifications'] = {
            'on_failure': [config['notifications'].get('email', 'data-team@company.com')]
        }
    
    dab['resources']['jobs'][f'{job_name}_job'] = job
    
    logger.info(f"Generated configuration with {len(tasks)} tasks")
    return dab


def save_databricks_yml(output_path: str, config: Dict[str, Any]) -> None:
    """Save databricks.yml configuration to file."""
    try:
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False, indent=2)
        logger.info(f"Saved databricks.yml to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to save databricks.yml: {e}")
        raise


def main():
    """Main entry point for bundle generation."""
    if len(sys.argv) < 2:
        print("Usage: python generate_bundle.py <pipeline_name>")
        sys.exit(1)
    
    pipeline_name = sys.argv[1]
    pipeline_dir = Path(f"pipelines/{pipeline_name}")
    
    if not pipeline_dir.exists():
        logger.error(f"Pipeline directory not found: {pipeline_dir}")
        sys.exit(1)
    
    # Find config file
    config_file = None
    for ext in ['yml', 'yaml', 'json']:
        candidate = pipeline_dir / f"config.{ext}"
        if candidate.exists():
            config_file = candidate
            break
    
    if not config_file:
        logger.error(f"No config file found in {pipeline_dir}")
        sys.exit(1)
    
    # Load config and generate bundle
    config = load_config(str(config_file))
    dab_config = generate_databricks_yml(pipeline_name, config)
    
    # Save to databricks.yml
    output_file = pipeline_dir / "databricks.yml"
    save_databricks_yml(str(output_file), dab_config)
    
    logger.info("✅ Bundle generation complete!")


if __name__ == "__main__":
    main()


