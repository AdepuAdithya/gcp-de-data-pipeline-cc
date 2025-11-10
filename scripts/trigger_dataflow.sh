#!/bin/bash

# Script to trigger Dataflow jobs with proper error handling

echo "=== Starting Dataflow Job ==="

# Arguments
SCRIPT_PATH=$1
JOB_NAME=$2
INPUT=$3
OUTPUT=$4

# Configuration
PROJECT_ID="famous-athlete-476816-f8"
REGION="us-central1"
TEMP_LOCATION="gs://dataflow-temp-famous-athlete/temp/"
STAGING_LOCATION="gs://dataflow-temp-famous-athlete/staging/"
SERVICE_ACCOUNT="889802921645-compute@developer.gserviceaccount.com"
MACHINE_TYPE="n1-standard-2"
MAX_WORKERS=4

# Generate unique job name with timestamp
UNIQUE_JOB_NAME="${JOB_NAME}-$(date +%Y%m%d%H%M%S)"

echo "Job Name: ${UNIQUE_JOB_NAME}"
echo "Script: ${SCRIPT_PATH}"
echo "Input: ${INPUT}"
echo "Output: ${OUTPUT}"

# Execute Dataflow job
python "${SCRIPT_PATH}" \
    --project=${PROJECT_ID} \
    --region=${REGION} \
    --runner=DataflowRunner \
    --job_name=${UNIQUE_JOB_NAME} \
    --temp_location=${TEMP_LOCATION} \
    --staging_location=${STAGING_LOCATION} \
    --input=${INPUT} \
    --output=${OUTPUT} \
    --service_account_email=${SERVICE_ACCOUNT} \
    --machine_type=${MACHINE_TYPE} \
    --max_num_workers=${MAX_WORKERS} \
    --autoscaling_algorithm=THROUGHPUT_BASED

# Check exit status
if [ $? -eq 0 ]; then
    echo "SUCCESS: Dataflow job ${UNIQUE_JOB_NAME} completed"
    exit 0
else
    echo "ERROR: Dataflow job ${UNIQUE_JOB_NAME} failed"
    exit 1
fi
