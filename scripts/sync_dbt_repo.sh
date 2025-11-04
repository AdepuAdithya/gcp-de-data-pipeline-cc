#!/bin/bash

# Script to sync dbt repository to Composer environment

echo "=== Syncing dbt Repository ==="

# Configuration
DBT_REPO_URL="https://github.com/your-username/gcp-de-data-pipeline-dbt.git"
DBT_BRANCH="main"
LOCAL_DIR="/tmp/dbt-repo"
COMPOSER_BUCKET="us-central1-gcp-de-data-pip-8156fd48-bucket"
GCS_DEST="gs://${COMPOSER_BUCKET}/data/dbt/gcp_de_data_pipeline_dbt"

# Clone or pull latest
if [ -d "${LOCAL_DIR}" ]; then
    echo "Pulling latest changes from ${DBT_BRANCH}..."
    cd ${LOCAL_DIR} && git pull origin ${DBT_BRANCH}
else
    echo "Cloning repository..."
    git clone -b ${DBT_BRANCH} ${DBT_REPO_URL} ${LOCAL_DIR}
fi

# Check if clone/pull was successful
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to sync git repository"
    exit 1
fi

# Sync to GCS
echo "Syncing to ${GCS_DEST}..."
gsutil -m rsync -r -d ${LOCAL_DIR} ${GCS_DEST}/

if [ $? -eq 0 ]; then
    echo "SUCCESS: dbt repository synced successfully"
else
    echo "ERROR: Failed to sync to GCS"
    exit 1
fi

# Clean up
rm -rf ${LOCAL_DIR}
