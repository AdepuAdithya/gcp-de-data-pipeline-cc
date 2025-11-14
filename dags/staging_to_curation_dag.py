from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import yaml

CONFIG_PATH = '/home/airflow/gcs/dags/config/pipeline_config.yaml'

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

config = load_config()

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

with DAG(
    dag_id='staging_to_curation_pipeline',
    default_args=default_args,
    description='Transform Staging to Curation with quality checks and archiving',
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['employee', 'dbt', 'curation', 'production'],
    max_active_runs=1,
) as dag:

    # STAGE 3 (part): Transform Staging to Curation if applicable
    # This example assumes dbt handles curation after staging, so no explicit dataflow BashOperator here.
    # If there was a Dataflow job for staging to curation, you could add BashOperator here similarly.

    # STAGE 4: Data Quality Checks
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

    # STAGE 5: dbt Curation Run
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

    # STAGE 6: Archive Processed Files
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

    # Define the flow between stages
    data_quality >> run_dbt >> archive
    