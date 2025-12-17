# CI/CD Deployment Guide - Loyalty 2.0 DAB Framework

**Production-ready CI/CD setup for deploying DAB pipelines with Git integration**

## 📋 Overview

This guide covers deploying Databricks Asset Bundles using **GitHub Actions** with Git-integrated notebooks. This is the recommended approach for production deployments.

### Why GitHub Actions + Git Integration?

✅ **Version Control** - All notebooks tracked in Git  
✅ **Automated Deployment** - Push to deploy automatically  
✅ **Environment Promotion** - dev → staging → prod workflow  
✅ **Audit Trail** - Full deployment history  
✅ **Code Review** - PR-based deployment approval  
✅ **Rollback** - Easy rollback via Git  

---

## 🚀 Quick Start

### Prerequisites

1. **GitHub Repository** with this code
2. **Databricks Workspace** access
3. **GitHub Secrets** configured (see below)

### Setup Steps

#### 1. Configure GitHub Secrets

Go to your repository: **Settings → Secrets and variables → Actions → New repository secret**

Add these secrets:

| Secret Name | Description | Example |
|------------|-------------|---------|
| `DATABRICKS_HOST` | Your Databricks workspace URL | `https://adb-xxx.azuredatabricks.net/` |
| `DATABRICKS_TOKEN` | Service principal or PAT token | `dapi...` |

**For multiple environments**, create environment-specific secrets:
- Go to **Settings → Environments**
- Create environments: `dev`, `staging`, `prod`
- Add environment-specific secrets:
  - `DATABRICKS_HOST`
  - `DATABRICKS_TOKEN`

#### 2. Push Code to Git

```bash
# Navigate to repository root
cd /path/to/databricks-ai-utils

# Add workflows
git add .github/workflows/

# Add your pipeline
git add loyalty2.0_dab_framework/pipelines/kafka_traffic_pipeline_git/

# Commit and push
git commit -m "Add CI/CD workflows and kafka_traffic_pipeline_git"
git push origin dev
```

#### 3. Watch Automatic Deployment

- Go to **Actions** tab in GitHub
- See your workflow running
- Monitor deployment progress
- Check Databricks workspace for deployed job

---

## 📂 Workflow Files

### 1. `deploy-dab-pipelines.yml` (Automatic Deployment)

**Triggers on:**
- Push to `dev`, `staging`, or `main` branches
- Changes in `loyalty2.0_dab_framework/pipelines/**`
- Manual trigger via workflow_dispatch

**What it does:**
1. Detects which pipelines changed
2. Determines target environment from branch
3. Generates `databricks.yml` with Git integration
4. Validates bundle configuration
5. Deploys to Databricks
6. Posts deployment summary

**Branch → Environment Mapping:**
```
dev branch     → dev environment
staging branch → staging environment  
main branch    → prod environment
```

### 2. `manual-deploy.yml` (Manual Deployment)

**Triggers on:**
- Manual trigger only (via GitHub Actions UI)

**What it does:**
- Deploy specific pipeline to chosen environment
- Skip Git validation (since we're deploying FROM Git)
- Full control over deployment
- Detailed deployment summary

---

## 🔄 Deployment Workflows

### Workflow 1: Automatic Deployment (Recommended)

```bash
# 1. Create/modify pipeline
cd loyalty2.0_dab_framework/pipelines/kafka_traffic_pipeline_git

# 2. Make changes to notebooks or config
vim notebooks/bronze_to_silver/traffic_data_clean.py
vim config.json

# 3. Commit and push to dev branch
git add .
git commit -m "Update traffic data transformation logic"
git push origin dev

# 4. GitHub Actions automatically:
#    ✅ Detects changes in kafka_traffic_pipeline_git
#    ✅ Generates databricks.yml with Git source
#    ✅ Validates bundle
#    ✅ Deploys to dev environment
#    ✅ Job in Databricks pulls from Git dev branch

# 5. Verify deployment
#    - Check GitHub Actions tab for status
#    - Check Databricks workspace for job
```

### Workflow 2: Manual Deployment

```bash
# 1. Go to GitHub repository
# 2. Click "Actions" tab
# 3. Select "Manual Pipeline Deployment"
# 4. Click "Run workflow"
# 5. Fill in:
#    - Pipeline: kafka_traffic_pipeline_git
#    - Environment: dev
#    - Skip validation: true (recommended)
# 6. Click "Run workflow"
# 7. Monitor deployment progress
```

### Workflow 3: Environment Promotion (dev → staging → prod)

```bash
# Deploy to DEV
git checkout dev
git push origin dev  # Auto-deploys to dev

# After testing, promote to STAGING
git checkout staging
git merge dev
git push origin staging  # Auto-deploys to staging

# After validation, promote to PROD
git checkout main
git merge staging
git push origin main  # Auto-deploys to prod
```

---

## 🏗️ Pipeline Configuration for Git Integration

### Example: kafka_traffic_pipeline_git/config.json

```json
{
  "pipeline_name": "[Loyalty2.0] Traffic Data ETL Pipeline (Git)",
  "catalog": "dna_dev",
  "execution_sequence": [...],
  
  "source_config": {
    "default_source": "WORKSPACE",
    "git_config": {
      "git_url": "git@github.gapinc.com:bia-core-data-platform/databricks-ai-utils.git",
      "git_provider": "gitHubEnterprise"
    },
    "environment_overrides": {
      "dev": "GIT",
      "staging": "GIT",
      "prod": "GIT"
    }
  }
}
```

**Key Points:**
- `environment_overrides` enables Git integration per environment
- `git_url` points to your GitHub Enterprise repository
- Branch is auto-selected: dev→dev, staging→staging, prod→main

---

## 📊 What Gets Generated

### Generated databricks.yml Structure

```yaml
resources:
  jobs:
    loyalty20_traffic_data_etl_pipeline_git:
      name: '[Loyalty2.0] Traffic Data ETL Pipeline (Git)'
      tasks:
      - task_key: step_0_setup_databases
        notebook_task:
          notebook_path: notebooks/setup/setup_databases.py
          source: GIT                    # ← Notebook pulled from Git
          git_source:                    # ← Git configuration
            git_url: git@github.gapinc.com:bia-core-data-platform/databricks-ai-utils.git
            git_branch: dev              # ← Environment-specific branch
            git_provider: gitHubEnterprise
```

**Benefits:**
- Notebooks always in sync with Git
- No manual file uploads needed
- Full version control
- Easy rollback via Git

---

## 🔐 Security Best Practices

### 1. Use Service Principal (Recommended)

```bash
# Create service principal in Azure AD
az ad sp create-for-rbac --name "databricks-cicd-sp"

# Grant permissions in Databricks
# Use SP application ID and secret for DATABRICKS_TOKEN
```

### 2. Use Environment-Specific Secrets

```yaml
# In GitHub repository settings
Environments:
  - dev:
      DATABRICKS_HOST: https://dev-workspace.azuredatabricks.net
      DATABRICKS_TOKEN: dapi-dev-token
  
  - staging:
      DATABRICKS_HOST: https://staging-workspace.azuredatabricks.net
      DATABRICKS_TOKEN: dapi-staging-token
  
  - prod:
      DATABRICKS_HOST: https://prod-workspace.azuredatabricks.net
      DATABRICKS_TOKEN: dapi-prod-token
```

### 3. Branch Protection Rules

```bash
# Protect main and staging branches
Settings → Branches → Add rule:
  - Branch name pattern: main
  - Require pull request reviews: ✅
  - Require status checks: ✅
  - Include administrators: ✅
```

---

## 🐛 Troubleshooting

### Issue 1: "Cannot access Git repository"

**In Local Deployment:**
```
ERROR: Cannot access Git repository
```

**Solution:** This is expected locally. Git validation will pass in GitHub Actions because:
- GitHub Actions has access to the repository
- Databricks workspace has Git credentials configured
- Skip local Git validation when deploying via CI/CD

### Issue 2: "Databricks authentication failed"

**Cause:** Incorrect or missing secrets

**Solution:**
```bash
# Verify secrets are set
# Go to: Settings → Secrets → Actions
# Check: DATABRICKS_HOST and DATABRICKS_TOKEN exist

# Test connection manually
databricks workspace list / --host $DATABRICKS_HOST --token $DATABRICKS_TOKEN
```

### Issue 3: "Pipeline not found"

**Cause:** Pipeline path incorrect or not committed

**Solution:**
```bash
# Verify pipeline exists in Git
git ls-files | grep "loyalty2.0_dab_framework/pipelines/kafka_traffic_pipeline_git"

# If not found, add and commit
git add loyalty2.0_dab_framework/pipelines/kafka_traffic_pipeline_git/
git commit -m "Add pipeline"
git push
```

### Issue 4: "databricks.yml generation failed"

**Cause:** Invalid config.json or missing generator script

**Solution:**
```bash
# Validate config.json syntax
cd loyalty2.0_dab_framework
python3 -c "import json; json.load(open('pipelines/kafka_traffic_pipeline_git/config.json'))"

# Verify generator exists
ls -la src/utils/generate_dab.py
ls -la src/utils/generate_dab_streaming.py
```

---

## 📈 Monitoring Deployments

### GitHub Actions Dashboard

1. Go to **Actions** tab
2. See all deployment runs
3. Click on run for details
4. Download artifacts (databricks.yml, summary.txt)

### Databricks Workspace

1. Go to **Workflows** → **Jobs**
2. Find job: `[Loyalty2.0] Traffic Data ETL Pipeline (Git)`
3. Click **Runs** tab
4. Monitor execution

### Deployment Artifacts

Each deployment saves:
- `databricks.yml` (generated bundle config)
- `summary.txt` (deployment summary)
- Retention: 30 days

Download from: **Actions → Workflow run → Artifacts**

---

## 🎯 Best Practices

### 1. Use Pull Requests for Production

```bash
# Never push directly to main
# Always use PR workflow:

git checkout -b feature/update-transformation
# ... make changes ...
git commit -m "Update transformation logic"
git push origin feature/update-transformation

# Create PR: feature/update-transformation → main
# Request reviews
# CI/CD validates in PR checks
# Merge after approval
# Auto-deploys to prod
```

### 2. Test in Dev First

```bash
# Always deploy to dev first
git checkout dev
# ... make changes ...
git push origin dev

# Test in dev environment
# If successful, promote to staging
git checkout staging
git merge dev
git push origin staging
```

### 3. Tag Production Releases

```bash
# After prod deployment
git checkout main
git tag -a v1.0.0 -m "Production release 1.0.0"
git push origin v1.0.0

# Easy rollback to tagged version if needed
git checkout v1.0.0
```

### 4. Monitor Deployment Notifications

Set up GitHub notifications:
- **Settings → Notifications**
- Enable workflow run notifications
- Get alerts on deployment failures

---

## 🔄 Rollback Strategy

### Option 1: Rollback via Git (Recommended)

```bash
# Find last working commit
git log --oneline

# Revert to previous version
git revert <commit-hash>
git push origin main

# OR reset to previous commit (use cautiously)
git reset --hard <commit-hash>
git push --force origin main  # Requires admin rights
```

### Option 2: Rollback via Manual Deployment

```bash
# 1. Go to GitHub Actions
# 2. Select "Manual Pipeline Deployment"
# 3. Deploy specific Git commit/tag
# 4. Specify environment
# 5. Run workflow
```

---

## 📞 Support

### Deployment Issues

1. Check GitHub Actions logs
2. Verify secrets configuration
3. Test Databricks CLI connection
4. Review `databricks.yml` generation logs

### Pipeline Issues

1. Check Databricks job logs
2. Verify notebook syntax in Git
3. Test notebooks manually in Databricks
4. Review config.json configuration

---

## 📚 Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Databricks Git Integration](https://docs.databricks.com/repos/index.html)
- [Loyalty 2.0 DAB Framework README](./README.md)
- [Architecture Document](./ARCHITECTURE.md)

---

## ✅ Deployment Checklist

Before deploying to production:

- [ ] Code reviewed and approved
- [ ] Tested in dev environment
- [ ] Tested in staging environment
- [ ] databricks.yml generated successfully
- [ ] Bundle validation passed
- [ ] Git integration configured correctly
- [ ] Secrets configured for target environment
- [ ] Monitoring and alerts set up
- [ ] Rollback plan documented
- [ ] Team notified of deployment

---

**Built with ❤️ following Databricks & Microsoft CI/CD best practices**

