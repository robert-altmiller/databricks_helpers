#!/bin/bash

###############################################################################
# Universal Deploy Script for Loyalty 2.0 DAB Framework
# Supports: Streaming (tasks config) + Batch SQL-driven (execution_sequence)
# Usage: ./deploy.sh <pipeline_name> <target> <profile>
###############################################################################

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_step() { echo -e "${BLUE}[STEP]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_header() { echo -e "${CYAN}$1${NC}"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# Check arguments
if [ $# -lt 1 ]; then
    echo "============================================================================"
    print_header "  🚀 Loyalty 2.0 DAB Framework - Universal Deploy"
    echo "============================================================================"
    echo ""
    echo "Usage: ./deploy.sh <pipeline_name> [target] [profile]"
    echo ""
    echo "Supported Pipeline Types:"
    echo "  📊 Batch SQL-driven  - Bronze → Silver → Gold (execution_sequence)"
    echo "  🌊 Streaming         - Kafka → Postgres/Delta (tasks config)"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh kafka_to_lakebase dev          # Streaming pipeline"
    echo "  ./deploy.sh my_batch_pipeline dev          # Batch SQL pipeline"
    echo ""
    echo "============================================================================"
    exit 1
fi

PIPELINE_NAME="$1"
TARGET="${2:-dev}"
PROFILE="${3:-DEFAULT}"

echo "============================================================================"
print_header "  🚀 Deploying: $PIPELINE_NAME"
echo "============================================================================"
echo ""

# Navigate to project root
cd "$(dirname "$0")/.."

# Check if pipeline exists
if [ ! -d "pipelines/$PIPELINE_NAME" ]; then
    print_error "Pipeline not found: pipelines/$PIPELINE_NAME"
    exit 1
fi

# Find config file
CONFIG_FILE=""
if [ -f "pipelines/$PIPELINE_NAME/config/config.json" ]; then
    CONFIG_FILE="pipelines/$PIPELINE_NAME/config/config.json"
elif [ -f "pipelines/$PIPELINE_NAME/config.json" ]; then
    CONFIG_FILE="pipelines/$PIPELINE_NAME/config.json"
else
    print_error "No config.json found in pipelines/$PIPELINE_NAME"
    exit 1
fi

print_info "Config: $CONFIG_FILE"
print_info "Target: $TARGET"
print_info "Profile: $PROFILE"
echo ""

# Detect pipeline type
print_step "Step 1: Detecting pipeline type..."

PIPELINE_TYPE=$(python3 - "$CONFIG_FILE" << 'DETECT_SCRIPT'
import json
import sys

with open(sys.argv[1], 'r') as f:
    config = json.load(f)

if 'tasks' in config:
    print('streaming_tasks')
elif 'execution_sequence' in config:
    print('batch_sql')
elif 'layers' in config:
    print('batch_sql')
else:
    print('unknown')
DETECT_SCRIPT
)

if [ "$PIPELINE_TYPE" == "unknown" ]; then
    print_error "Cannot detect pipeline type from config"
    exit 1
fi

print_info "Detected: $PIPELINE_TYPE"
echo ""

# Step 2: Generate databricks.yml
print_step "Step 2: Generating databricks.yml..."

# Export TARGET_ENV so generators can determine source type
export TARGET_ENV=$TARGET

if [ "$PIPELINE_TYPE" == "streaming_tasks" ]; then
    # Use streaming generator
    print_info "Using streaming generator for pipeline"
    
    if [ ! -f "src/utils/generate_dab_streaming.py" ]; then
        print_error "Generator script not found: src/utils/generate_dab_streaming.py"
        exit 1
    fi
    
    python3 src/utils/generate_dab_streaming.py $PIPELINE_NAME

elif [ "$PIPELINE_TYPE" == "batch_sql" ]; then
    # Use batch SQL generator
    print_info "Using batch SQL generator for pipeline"
    
    if [ ! -f "src/utils/generate_dab.py" ]; then
        print_error "Generator script not found: src/utils/generate_dab.py"
        exit 1
    fi
    
    python3 src/utils/generate_dab.py $PIPELINE_NAME
fi

if [ ! -f "pipelines/$PIPELINE_NAME/databricks.yml" ]; then
    print_error "Failed to generate databricks.yml"
    exit 1
fi

print_info "✅ databricks.yml generated"
echo ""

# Step 2.5: Validate Git source if applicable
# Determine source type based on target environment
SOURCE_TYPE=$(python3 - "$CONFIG_FILE" "$TARGET" << 'CHECK_SOURCE'
import json
import sys

with open(sys.argv[1], 'r') as f:
    config = json.load(f)

target_env = sys.argv[2]
source_config = config.get('source_config', {})
env_overrides = source_config.get('environment_overrides', {})
source_type = env_overrides.get(target_env, source_config.get('default_source', 'WORKSPACE'))

print(source_type)
CHECK_SOURCE
)

if [ "$SOURCE_TYPE" == "GIT" ]; then
    print_step "Step 2.5: Validating Git source..."
    
    # Extract Git configuration
    GIT_CONFIG=$(python3 - "$CONFIG_FILE" "$TARGET" << 'GET_GIT_CONFIG'
import json
import sys

with open(sys.argv[1], 'r') as f:
    config = json.load(f)

target_env = sys.argv[2]
source_config = config.get('source_config', {})
git_config = source_config.get('git_config', {})

# Branch mapping
git_branch_map = {'dev': 'dev', 'staging': 'staging', 'prod': 'main'}

git_url = git_config.get('git_url', '')
git_branch = git_branch_map.get(target_env, 'main')

print(f"{git_url}|{git_branch}")
GET_GIT_CONFIG
)
    
    GIT_URL=$(echo "$GIT_CONFIG" | cut -d'|' -f1)
    GIT_BRANCH=$(echo "$GIT_CONFIG" | cut -d'|' -f2)
    
    # Get pipeline path relative to repo root
    PIPELINE_REL_PATH="pipelines/$PIPELINE_NAME"
    
    # Run validation script
    if ! bash scripts/validate-git-source.sh "$GIT_URL" "$GIT_BRANCH" "$PIPELINE_REL_PATH" "$CONFIG_FILE"; then
        print_error "Git source validation failed"
        exit 1
    fi
    
    print_info "✅ Git source validation passed"
    echo ""
else
    print_info "Using WORKSPACE source - skipping Git validation"
    echo ""
fi

# Step 3: Validate bundle
print_step "Step 3: Validating bundle..."
cd pipelines/$PIPELINE_NAME

if [ "$PROFILE" == "DEFAULT" ]; then
    databricks bundle validate -t $TARGET
else
    databricks bundle validate -t $TARGET --profile $PROFILE
fi

print_info "✅ Bundle validation successful"
echo ""

# Step 4: Deploy
print_step "Step 4: Deploying to Databricks..."

if [ "$PROFILE" == "DEFAULT" ]; then
    databricks bundle deploy -t $TARGET
else
    databricks bundle deploy -t $TARGET --profile $PROFILE
fi

print_info "✅ Deployed successfully"
echo ""

# Step 5: Summary
print_step "Step 5: Deployment Summary"

if [ "$PROFILE" == "DEFAULT" ]; then
    databricks bundle summary -t $TARGET
else
    databricks bundle summary -t $TARGET --profile $PROFILE
fi

echo ""
echo "============================================================================"
print_header "  ✅ Deployment Complete!"
echo "============================================================================"
print_info "Pipeline: $PIPELINE_NAME ($PIPELINE_TYPE)"
print_info "Environment: $TARGET"
echo ""
print_info "To run:"
echo "  cd pipelines/$PIPELINE_NAME"
if [ "$PROFILE" == "DEFAULT" ]; then
    echo "  databricks bundle run <job_name> -t $TARGET"
else
    echo "  databricks bundle run <job_name> -t $TARGET --profile $PROFILE"
fi
echo "============================================================================"

# Return to project root
cd ../..
