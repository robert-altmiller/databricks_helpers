#!/bin/bash

###############################################################################
# Git Source Validation Script
# Validates Git repository connectivity and file existence before deployment
# Usage: validate-git-source.sh <git_url> <git_branch> <pipeline_path> <config_file>
###############################################################################

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_check() { echo -e "${GREEN}✓${NC} $1"; }

# Arguments
GIT_URL="$1"
GIT_BRANCH="$2"
PIPELINE_PATH="$3"
CONFIG_FILE="$4"

if [ -z "$GIT_URL" ] || [ -z "$GIT_BRANCH" ] || [ -z "$PIPELINE_PATH" ] || [ -z "$CONFIG_FILE" ]; then
    print_error "Missing required arguments"
    echo "Usage: $0 <git_url> <git_branch> <pipeline_path> <config_file>"
    exit 1
fi

echo ""
echo "============================================================================"
echo "  Git Source Validation"
echo "============================================================================"
print_info "Repository: $GIT_URL"
print_info "Branch: $GIT_BRANCH"
print_info "Pipeline: $PIPELINE_PATH"
echo ""

VALIDATION_FAILED=0

# Step 1: Check if git is available
if ! command -v git &> /dev/null; then
    print_error "Git is not installed or not in PATH"
    exit 1
fi
print_check "Git command available"

# Step 2: Test Git repository connectivity
print_info "Testing Git repository connectivity..."

# Try to list remote refs (this tests connectivity without cloning)
if git ls-remote "$GIT_URL" &> /dev/null; then
    print_check "Git repository is accessible"
else
    print_error "Cannot access Git repository: $GIT_URL"
    print_error "Please check:"
    echo "  - Repository URL is correct"
    echo "  - You have access permissions"
    echo "  - SSH keys are configured (for SSH URLs)"
    echo "  - Network connectivity"
    exit 1
fi

# Step 3: Check if branch exists
print_info "Checking if branch exists: $GIT_BRANCH"

if git ls-remote --heads "$GIT_URL" "$GIT_BRANCH" | grep -q "$GIT_BRANCH"; then
    print_check "Branch '$GIT_BRANCH' exists"
else
    print_error "Branch '$GIT_BRANCH' does not exist in repository"
    print_error "Available branches:"
    git ls-remote --heads "$GIT_URL" | sed 's/.*refs\/heads\//  - /'
    exit 1
fi

# Step 4: Validate notebook files exist in Git
print_info "Validating notebook files in Git repository..."

# Create temporary directory for sparse checkout
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

cd "$TEMP_DIR"

# Initialize git and setup sparse checkout
git init -q
git remote add origin "$GIT_URL"
git config core.sparseCheckout true

# Add pipeline path to sparse checkout
echo "$PIPELINE_PATH/*" >> .git/info/sparse-checkout

# Fetch only the specified branch
print_info "Fetching branch $GIT_BRANCH (sparse checkout)..."
if ! git fetch --depth 1 origin "$GIT_BRANCH" &> /dev/null; then
    print_error "Failed to fetch branch $GIT_BRANCH"
    exit 1
fi

git checkout -q FETCH_HEAD

# Check if pipeline directory exists
if [ ! -d "$PIPELINE_PATH" ]; then
    print_error "Pipeline directory not found in Git: $PIPELINE_PATH"
    exit 1
fi

print_check "Pipeline directory exists in Git"

# Extract notebook paths from config
NOTEBOOK_PATHS=$(python3 - "$CONFIG_FILE" << 'EXTRACT_NOTEBOOKS'
import json
import sys

with open(sys.argv[1], 'r') as f:
    config = json.load(f)

notebooks = []

# For streaming pipelines with tasks
if 'tasks' in config:
    for task in config['tasks']:
        if 'notebook_path' in task:
            notebooks.append(task['notebook_path'])

# For batch pipelines with execution_sequence
if 'execution_sequence' in config:
    for step in config['execution_sequence']:
        if 'notebook_path' in step:
            notebooks.append(step['notebook_path'])

for nb in notebooks:
    print(nb)
EXTRACT_NOTEBOOKS
)

if [ -z "$NOTEBOOK_PATHS" ]; then
    print_warning "No notebooks found in config to validate"
else
    MISSING_FILES=0
    while IFS= read -r notebook; do
        FULL_PATH="$PIPELINE_PATH/$notebook"
        if [ -f "$FULL_PATH" ]; then
            print_check "$notebook"
        else
            print_error "MISSING: $notebook"
            MISSING_FILES=$((MISSING_FILES + 1))
            VALIDATION_FAILED=1
        fi
    done <<< "$NOTEBOOK_PATHS"
    
    if [ $MISSING_FILES -gt 0 ]; then
        print_error "$MISSING_FILES notebook file(s) missing in Git repository"
        echo ""
        print_error "Files must exist in Git before deploying with GIT source"
        exit 1
    fi
fi

echo ""
echo "============================================================================"
if [ $VALIDATION_FAILED -eq 0 ]; then
    print_info "✅ Git source validation passed"
    echo "============================================================================"
    exit 0
else
    print_error "❌ Git source validation failed"
    echo "============================================================================"
    exit 1
fi
