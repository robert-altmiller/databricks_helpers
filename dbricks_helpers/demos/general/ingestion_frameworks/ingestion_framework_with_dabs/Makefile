.PHONY: help install validate test clean deploy

# Default target
.DEFAULT_GOAL := help

# Colors
BLUE := \033[0;34m
GREEN := \033[0;32m
RED := \033[0;31m
NC := \033[0m

help: ## Show this help message
	@echo '$(BLUE)Loyalty 2.0 DAB Framework - Commands$(NC)'
	@echo ''
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ''

install: ## Install dependencies
	@echo "$(BLUE)Installing dependencies...$(NC)"
	pip install -r requirements.txt
	@echo "$(GREEN)✓ Installation complete!$(NC)"

validate: ## Validate bundle configuration
	@echo "$(BLUE)Validating bundle...$(NC)"
	databricks bundle validate -t dev
	@echo "$(GREEN)✓ Validation successful!$(NC)"

test: ## Run tests
	@echo "$(BLUE)Running tests...$(NC)"
	pytest tests/ -v
	@echo "$(GREEN)✓ Tests passed!$(NC)"

clean: ## Clean generated files
	@echo "$(BLUE)Cleaning up...$(NC)"
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -type d -name '.pytest_cache' -delete
	@echo "$(GREEN)✓ Cleanup complete!$(NC)"

deploy: ## Deploy pipeline (use: make deploy PIPELINE=my_pipeline ENV=dev)
	@if [ -z "$(PIPELINE)" ]; then \
		echo "$(RED)Error: PIPELINE name required$(NC)"; \
		echo "Usage: make deploy PIPELINE=my_pipeline ENV=dev"; \
		exit 1; \
	fi
	./scripts/deploy.sh $(PIPELINE) $(or $(ENV),dev)

setup: ## Initial setup
	@echo "$(BLUE)Running initial setup...$(NC)"
	./scripts/setup.sh
	@echo "$(GREEN)✓ Setup complete!$(NC)"

list-pipelines: ## List all available pipelines
	@echo "$(BLUE)Available Pipelines:$(NC)"
	@ls -1 pipelines/ | grep -v template | sed 's/^/  - /'

info: ## Show project information
	@echo "$(BLUE)Loyalty 2.0 DAB Framework$(NC)"
	@echo ""
	@echo "Python version: $$(python --version)"
	@echo "Databricks CLI: $$(databricks --version 2>/dev/null || echo 'Not installed')"
	@echo ""
	@echo "Project structure:"
	@find . -maxdepth 2 -type d | grep -v -E "(\.git|__pycache__|\.pytest)" | head -20

