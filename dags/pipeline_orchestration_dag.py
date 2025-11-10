"""
Employee Data Pipeline Orchestration
Orchestrates: GCS -> Raw -> Staging -> Curation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import yaml
import os

# Load configuration
CONFIG_PATH = '/home/airflow/gcs/dags/config/pipeline_config.yaml'

def load_config():
    """Load pipeline configuration"""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': [config['notifications']['email']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Helper function to build Dataflow command
def build_dataflow_command(job_config, input_path, output_table):
    """Build Dataflow job command"""
    script_path = f"gs://{config['repositories']['dataflow']['gcs_bucket']}/{config['repositories']['dataflow']['gcs_path']}{job_config['script']}"
    local_script = f"/tmp/{job_config['script']}"
    
    return f"""
    gsutil cp {script_path} {local_script} && \
    python {local_script} \
        --project={config['project_id']} \
        --region={config['region']} \
        --runner=DataflowRunner \
        --job_name={job_config['job_name']}-$(date +%Y%m%d-%H%M%S) \
        --temp_location={config['dataflow']['temp_location']} \
        --staging_location={config['dataflow']['staging_location']} \
        --input={input_path} \
        --output={output_table} \
        --service_account_email={config['dataflow']['service_account']} \
        --machine_type={config['dataflow']['machine_type']} \
        --max_num_workers={config['dataflow']['max_workers']}
    """

# DAG definition
with DAG(
    dag_id='employee_data_pipeline_v1',
    default_args=default_args,
    description='End-to-end Employee Data Pipeline: GCS -> Raw -> Staging -> Curation',
    schedule_interval= None,  # manual trigger only
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['employee', 'dataflow', 'dbt', 'production'],
    max_active_runs=1,
) as dag:

    # ========================================
    # STAGE 1: Wait for Source Files
    # ========================================
    
    with TaskGroup('wait_for_source_files', tooltip='Wait for CSV files to arrive') as wait_files:
        
        wait_employee = GCSObjectExistenceSensor(
            task_id='wait_for_employee_csv',
            bucket=config['gcs']['source_bucket'],
            object=f"{config['gcs']['landing_prefix']}{config['gcs']['files']['employee']['pattern']}",
            timeout=config['gcs']['files']['employee']['sensor_timeout'],
            poke_interval=config['gcs']['files']['employee']['poke_interval'],
            mode='poke',
        )
        
        wait_department = GCSObjectExistenceSensor(
            task_id='wait_for_department_csv',
            bucket=config['gcs']['source_bucket'],
            object=f"{config['gcs']['landing_prefix']}{config['gcs']['files']['department']['pattern']}",
            timeout=config['gcs']['files']['department']['sensor_timeout'],
            poke_interval=config['gcs']['files']['department']['poke_interval'],
            mode='poke',
        )

    # ========================================
    # STAGE 2: GCS to Raw (Dataflow)
    # ========================================
    
    with TaskGroup('ingest_to_raw', tooltip='Load CSV files to Raw BigQuery tables') as gcs_to_raw:
        
        employee_raw = BashOperator(
            task_id='employee_gcs_to_raw',
            bash_command=build_dataflow_command(
                config['dataflow']['jobs']['gcs_to_raw']['employee'],
                f"gs://{config['gcs']['source_bucket']}/{config['gcs']['landing_prefix']}{config['gcs']['files']['employee']['pattern']}",
                f"{config['project_id']}:{config['bigquery']['raw_dataset']}.{config['dataflow']['jobs']['gcs_to_raw']['employee']['table']}"
            ),
        )
        
        department_raw = BashOperator(
            task_id='department_gcs_to_raw',
            bash_command=build_dataflow_command(
                config['dataflow']['jobs']['gcs_to_raw']['department'],
                f"gs://{config['gcs']['source_bucket']}/{config['gcs']['landing_prefix']}{config['gcs']['files']['department']['pattern']}",
                f"{config['project_id']}:{config['bigquery']['raw_dataset']}.{config['dataflow']['jobs']['gcs_to_raw']['department']['table']}"
            ),
        )

    # ========================================
    # STAGE 3: Raw to Staging (Dataflow)
    # ========================================
    
    with TaskGroup('transform_to_staging', tooltip='Transform Raw to Staging tables') as raw_to_staging:
        
        employee_stg = BashOperator(
            task_id='employee_raw_to_staging',
            bash_command=build_dataflow_command(
                config['dataflow']['jobs']['raw_to_staging']['employee'],
                f"{config['project_id']}:{config['bigquery']['raw_dataset']}.{config['dataflow']['jobs']['gcs_to_raw']['employee']['table']}",
                f"{config['project_id']}:{config['bigquery']['staging_dataset']}.{config['dataflow']['jobs']['raw_to_staging']['employee']['table']}"
            ),
        )
        
        department_stg = BashOperator(
            task_id='department_raw_to_staging',
            bash_command=build_dataflow_command(
                config['dataflow']['jobs']['raw_to_staging']['department'],
                f"{config['project_id']}:{config['bigquery']['raw_dataset']}.{config['dataflow']['jobs']['gcs_to_raw']['department']['table']}",
                f"{config['project_id']}:{config['bigquery']['staging_dataset']}.{config['dataflow']['jobs']['raw_to_staging']['department']['table']}"
            ),
        )

    # ========================================
    # STAGE 4: Data Quality Checks
    # ========================================
    
    with TaskGroup('validate_staging_data', tooltip='Run data quality checks') as data_quality:
        
        check_employee = BigQueryCheckOperator(
            task_id='check_employee_count',
            sql=f"""
            SELECT COUNT(*) > 0 
            FROM `{config['project_id']}.{config['bigquery']['staging_dataset']}.{config['dataflow']['jobs']['raw_to_staging']['employee']['table']}`
            """,
            use_legacy_sql=False,
        )
        
        check_department = BigQueryCheckOperator(
            task_id='check_department_count',
            sql=f"""
            SELECT COUNT(*) > 0 
            FROM `{config['project_id']}.{config['bigquery']['staging_dataset']}.{config['dataflow']['jobs']['raw_to_staging']['department']['table']}`
            """,
            use_legacy_sql=False,
        )

    # ========================================
    # STAGE 5: dbt Curation
    # ========================================
    
    run_dbt = BashOperator(
        task_id='run_dbt_curation',
        bash_command=f"""
        cd {config['dbt']['project_dir']} && \
        dbt deps && \
        dbt run --profiles-dir {config['dbt']['profiles_dir']} \
                --target {config['dbt']['target']} \
                --select {config['dbt']['models']}
        """,
    )

    # ========================================
    # STAGE 6: Archive Files
    # ========================================
    
    with TaskGroup('archive_processed_files', tooltip='Move processed files to archive') as archive:
        
        archive_employee = GCSToGCSOperator(
            task_id='archive_employee_csv',
            source_bucket=config['gcs']['source_bucket'],
            source_object=f"{config['gcs']['landing_prefix']}{config['gcs']['files']['employee']['pattern']}",
            destination_bucket=config['gcs']['source_bucket'],
            destination_object=f"{config['gcs']['archive_prefix']}{config['gcs']['files']['employee']['pattern']}.{{{{ ds }}}}",
            move_object=True,
        )
        
        archive_department = GCSToGCSOperator(
            task_id='archive_department_csv',
            source_bucket=config['gcs']['source_bucket'],
            source_object=f"{config['gcs']['landing_prefix']}{config['gcs']['files']['department']['pattern']}",
            destination_bucket=config['gcs']['source_bucket'],
            destination_object=f"{config['gcs']['archive_prefix']}{config['gcs']['files']['department']['pattern']}.{{{{ ds }}}}",
            move_object=True,
        )

    # ========================================
    # Pipeline Flow
    # ========================================
    
    wait_files >> gcs_to_raw >> raw_to_staging >> data_quality >> run_dbt >> archive
