"""Basic tests for Loyalty 2.0 DAB Framework"""

import os
import yaml
from pathlib import Path


def test_project_structure():
    """Test that essential directories exist."""
    base_dir = Path(__file__).parent.parent
    
    required_dirs = [
        "config",
        "src/utils",
        "pipelines/template",
        "scripts",
        "tests"
    ]
    
    for dir_path in required_dirs:
        full_path = base_dir / dir_path
        assert full_path.exists(), f"Required directory missing: {dir_path}"
        assert full_path.is_dir(), f"Path is not a directory: {dir_path}"


def test_core_files_exist():
    """Test that core configuration files exist."""
    base_dir = Path(__file__).parent.parent
    
    required_files = [
        "databricks.yml",
        "requirements.txt",
        "README.md",
        "Makefile",
        ".gitignore"
    ]
    
    for file_path in required_files:
        full_path = base_dir / file_path
        assert full_path.exists(), f"Required file missing: {file_path}"
        assert full_path.is_file(), f"Path is not a file: {file_path}"


def test_environment_configs():
    """Test that environment configuration files exist and are valid YAML."""
    base_dir = Path(__file__).parent.parent
    config_dir = base_dir / "config"
    
    environments = ["dev", "staging", "prod"]
    
    for env in environments:
        config_file = config_dir / f"{env}.yml"
        assert config_file.exists(), f"Config missing: {env}.yml"
        
        # Validate YAML syntax
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            assert config is not None, f"Empty config: {env}.yml"
            assert "environment" in config, f"Missing 'environment' key in {env}.yml"
            assert config["environment"] == env, f"Environment mismatch in {env}.yml"


def test_databricks_yml_valid():
    """Test that databricks.yml is valid YAML."""
    base_dir = Path(__file__).parent.parent
    dab_file = base_dir / "databricks.yml"
    
    with open(dab_file, 'r') as f:
        config = yaml.safe_load(f)
        
    assert "bundle" in config, "Missing 'bundle' section"
    assert "name" in config["bundle"], "Missing bundle name"
    assert "targets" in config, "Missing 'targets' section"
    
    # Check environments
    required_targets = ["dev", "staging", "prod"]
    for target in required_targets:
        assert target in config["targets"], f"Missing target: {target}"


def test_scripts_executable():
    """Test that shell scripts have execute permissions."""
    base_dir = Path(__file__).parent.parent
    scripts_dir = base_dir / "scripts"
    
    scripts = ["deploy.sh", "setup.sh"]
    
    for script in scripts:
        script_path = scripts_dir / script
        assert script_path.exists(), f"Script missing: {script}"
        
        # Check if executable (on Unix-like systems)
        if os.name != 'nt':  # Skip on Windows
            assert os.access(script_path, os.X_OK), f"Script not executable: {script}"


def test_template_pipeline():
    """Test that template pipeline has required files."""
    base_dir = Path(__file__).parent.parent
    template_dir = base_dir / "pipelines" / "template"
    
    assert (template_dir / "config.yml").exists(), "Template config.yml missing"
    assert (template_dir / "notebooks").exists(), "Template notebooks dir missing"
    
    # Validate template config
    with open(template_dir / "config.yml", 'r') as f:
        config = yaml.safe_load(f)
        
    assert "pipeline_name" in config, "Template missing pipeline_name"
    assert "tasks" in config, "Template missing tasks"
    assert len(config["tasks"]) > 0, "Template has no tasks"


def test_python_utils():
    """Test that Python utility modules exist."""
    base_dir = Path(__file__).parent.parent
    utils_dir = base_dir / "src" / "utils"
    
    required_modules = [
        "__init__.py",
        "logger.py",
        "generate_bundle.py"
    ]
    
    for module in required_modules:
        module_path = utils_dir / module
        assert module_path.exists(), f"Utility module missing: {module}"


def test_requirements_file():
    """Test that requirements.txt has essential dependencies."""
    base_dir = Path(__file__).parent.parent
    req_file = base_dir / "requirements.txt"
    
    with open(req_file, 'r') as f:
        requirements = f.read()
    
    essential_packages = ["pyyaml", "databricks-cli"]
    
    for package in essential_packages:
        assert package in requirements.lower(), f"Missing package: {package}"


if __name__ == "__main__":
    # Run tests manually
    import sys
    
    tests = [
        test_project_structure,
        test_core_files_exist,
        test_environment_configs,
        test_databricks_yml_valid,
        test_scripts_executable,
        test_template_pipeline,
        test_python_utils,
        test_requirements_file
    ]
    
    print("Running tests...\n")
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            print(f"✓ {test_func.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"✗ {test_func.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test_func.__name__}: Unexpected error: {e}")
            failed += 1
    
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


