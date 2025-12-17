#!/bin/bash

###############################################################################
# Setup Script for Loyalty 2.0 DAB Framework
# Initializes the environment and validates prerequisites
###############################################################################

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_step() { echo -e "${BLUE}[STEP]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }

echo "============================================================================"
echo -e "${BLUE}Loyalty 2.0 DAB Framework - Setup${NC}"
echo "============================================================================"
echo ""

# Step 1: Check prerequisites
print_step "Step 1: Checking prerequisites..."

# Check Python
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is not installed"
    exit 1
fi
print_info "✓ Python $(python3 --version | cut -d' ' -f2)"

# Check pip
if ! command -v pip3 &> /dev/null; then
    print_error "pip3 is not installed"
    exit 1
fi
print_info "✓ pip $(pip3 --version | cut -d' ' -f2)"

# Check Databricks CLI
if ! command -v databricks &> /dev/null; then
    print_warning "Databricks CLI not found. Installing..."
    pip3 install databricks-cli
fi
print_info "✓ Databricks CLI installed"

echo ""

# Step 2: Install dependencies
print_step "Step 2: Installing Python dependencies..."
pip3 install -r requirements.txt
print_info "✓ Dependencies installed"
echo ""

# Step 3: Check Databricks configuration
print_step "Step 3: Checking Databricks configuration..."

if [ ! -f "$HOME/.databrickscfg" ]; then
    print_warning "Databricks config not found at ~/.databrickscfg"
    echo ""
    echo "To configure Databricks CLI, run:"
    echo "  databricks configure --token"
    echo ""
    echo "You'll need:"
    echo "  - Databricks Host: https://your-workspace.cloud.databricks.com"
    echo "  - Personal Access Token: (generate from User Settings > Access Tokens)"
    echo ""
    read -p "Would you like to configure now? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        databricks configure --token
    else
        print_warning "Skipping Databricks configuration"
    fi
else
    print_info "✓ Databricks CLI configured"
fi

echo ""

# Step 4: Validate bundle
print_step "Step 4: Validating bundle configuration..."
if databricks bundle validate -t dev 2>/dev/null; then
    print_info "✓ Bundle validation successful"
else
    print_warning "Bundle validation skipped (configure Databricks CLI first)"
fi

echo ""
echo "============================================================================"
echo -e "${GREEN}✓ Setup Complete!${NC}"
echo "============================================================================"
echo ""
echo "Next steps:"
echo "  1. Create a new pipeline:"
echo "     cp -r pipelines/template pipelines/my_pipeline"
echo ""
echo "  2. Edit configuration:"
echo "     vim pipelines/my_pipeline/config.yml"
echo ""
echo "  3. Deploy:"
echo "     ./scripts/deploy.sh my_pipeline dev"
echo ""
echo "For help: make help"
echo "============================================================================"


