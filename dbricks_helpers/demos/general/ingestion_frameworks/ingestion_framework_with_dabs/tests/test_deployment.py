"""
Simple deployment tests for Loyalty 2.0 DAB Framework
Tests the core functionality: pipeline type detection and databricks.yml generation
"""

import os
import sys
import json
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "utils"))

from generate_dab_streaming import generate_databricks_yml as generate_streaming
from generate_dab import generate_databricks_yml as generate_batch


def test_streaming_pipeline_generation():
    """
    Test Case 1: Verify streaming pipeline (kafka_to_lakebase) generates valid databricks.yml
    """
    print("\n" + "="*80)
    print("TEST 1: Streaming Pipeline Generation")
    print("="*80)
    
    pipeline_name = "kafka_to_lakebase"
    base_dir = Path(__file__).parent.parent
    
    # Check config exists
    config_file = base_dir / f"pipelines/{pipeline_name}/config/config.json"
    assert config_file.exists(), f"Config not found: {config_file}"
    print(f"✓ Config file exists: {config_file}")
    
    # Load and verify config has tasks
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    assert 'tasks' in config, "Config must have 'tasks' key for streaming pipeline"
    assert len(config['tasks']) > 0, "Config must have at least one task"
    print(f"✓ Config has {len(config['tasks'])} tasks")
    
    # Change to project root
    original_dir = os.getcwd()
    os.chdir(base_dir)
    
    try:
        # Generate databricks.yml
        success = generate_streaming(pipeline_name)
        assert success, "Failed to generate databricks.yml"
        print("✓ Generated databricks.yml successfully")
        
        # Verify output file exists
        output_file = base_dir / f"pipelines/{pipeline_name}/databricks.yml"
        assert output_file.exists(), f"Output file not created: {output_file}"
        print(f"✓ Output file exists: {output_file}")
        
        # Validate YAML structure
        with open(output_file, 'r') as f:
            dab_config = yaml.safe_load(f)
        
        # Check required keys
        assert 'bundle' in dab_config, "Missing 'bundle' key"
        assert 'resources' in dab_config, "Missing 'resources' key"
        assert 'jobs' in dab_config['resources'], "Missing 'jobs' in resources"
        print("✓ YAML structure is valid")
        
        # Check job has tasks
        jobs = dab_config['resources']['jobs']
        assert len(jobs) > 0, "No jobs defined"
        
        job_name = list(jobs.keys())[0]
        job = jobs[job_name]
        
        assert 'tasks' in job, "Job must have tasks"
        assert len(job['tasks']) > 0, "Job must have at least one task"
        print(f"✓ Job '{job_name}' has {len(job['tasks'])} tasks")
        
        # Check timeout is 0 for streaming
        assert job['timeout_seconds'] == 0, "Streaming pipeline should have timeout_seconds=0"
        print("✓ Timeout is 0 (continuous streaming)")
        
        print("\n✅ TEST 1 PASSED: Streaming pipeline generation works correctly")
        return True
        
    except Exception as e:
        print(f"\n❌ TEST 1 FAILED: {str(e)}")
        raise
    finally:
        os.chdir(original_dir)


def test_batch_pipeline_generation():
    """
    Test Case 2: Verify batch pipeline (kafka_traffic_pipeline) generates valid databricks.yml
    """
    print("\n" + "="*80)
    print("TEST 2: Batch Pipeline Generation")
    print("="*80)
    
    pipeline_name = "kafka_traffic_pipeline"
    base_dir = Path(__file__).parent.parent
    
    # Check config exists
    config_file = base_dir / f"pipelines/{pipeline_name}/config.json"
    assert config_file.exists(), f"Config not found: {config_file}"
    print(f"✓ Config file exists: {config_file}")
    
    # Load and verify config has execution_sequence
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    assert 'execution_sequence' in config, "Config must have 'execution_sequence' key for batch pipeline"
    assert len(config['execution_sequence']) > 0, "Config must have at least one step"
    print(f"✓ Config has {len(config['execution_sequence'])} steps")
    
    # Change to project root
    original_dir = os.getcwd()
    os.chdir(base_dir)
    
    try:
        # Generate databricks.yml
        success = generate_batch(pipeline_name)
        assert success, "Failed to generate databricks.yml"
        print("✓ Generated databricks.yml successfully")
        
        # Verify output file exists
        output_file = base_dir / f"pipelines/{pipeline_name}/databricks.yml"
        assert output_file.exists(), f"Output file not created: {output_file}"
        print(f"✓ Output file exists: {output_file}")
        
        # Validate YAML structure
        with open(output_file, 'r') as f:
            dab_config = yaml.safe_load(f)
        
        # Check required keys
        assert 'bundle' in dab_config, "Missing 'bundle' key"
        assert 'variables' in dab_config, "Missing 'variables' key"
        assert 'resources' in dab_config, "Missing 'resources' key"
        print("✓ YAML structure is valid")
        
        # Check variables
        variables = dab_config['variables']
        assert 'catalog' in variables, "Missing 'catalog' variable"
        assert 'bronze_database' in variables, "Missing 'bronze_database' variable"
        assert 'silver_database' in variables, "Missing 'silver_database' variable"
        assert 'gold_database' in variables, "Missing 'gold_database' variable"
        print("✓ All required variables defined")
        
        # Check job has tasks
        jobs = dab_config['resources']['jobs']
        assert len(jobs) > 0, "No jobs defined"
        
        job_name = list(jobs.keys())[0]
        job = jobs[job_name]
        
        assert 'tasks' in job, "Job must have tasks"
        num_tasks = len(job['tasks'])
        assert num_tasks > 0, "Job must have at least one task"
        print(f"✓ Job '{job_name}' has {num_tasks} tasks")
        
        # Check timeout is set for batch
        assert job['timeout_seconds'] > 0, "Batch pipeline should have timeout_seconds > 0"
        print(f"✓ Timeout is {job['timeout_seconds']} seconds (batch processing)")
        
        print("\n✅ TEST 2 PASSED: Batch pipeline generation works correctly")
        return True
        
    except Exception as e:
        print(f"\n❌ TEST 2 FAILED: {str(e)}")
        raise
    finally:
        os.chdir(original_dir)


if __name__ == "__main__":
    print("\n" + "="*80)
    print("LOYALTY 2.0 DAB FRAMEWORK - DEPLOYMENT TESTS")
    print("="*80)
    
    passed = 0
    failed = 0
    
    # Test 1: Streaming Pipeline
    try:
        test_streaming_pipeline_generation()
        passed += 1
    except Exception as e:
        print(f"Error: {e}")
        failed += 1
    
    # Test 2: Batch Pipeline
    try:
        test_batch_pipeline_generation()
        passed += 1
    except Exception as e:
        print(f"Error: {e}")
        failed += 1
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total:  {passed + failed}")
    print("="*80)
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! Framework is ready to use.")
        sys.exit(0)
    else:
        print(f"\n⚠️  {failed} test(s) failed. Please fix the issues.")
        sys.exit(1)

