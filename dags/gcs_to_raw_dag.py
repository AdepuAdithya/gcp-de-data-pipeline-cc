from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import yaml

# Load configuration
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

def build_dataflow_command(job_config, input_path, output_table):
    script_path = f"gs://{config['repositories']['dataflow']['gcs_bucket']}/{config['repositories']['dataflow']['gcs_path']}{job_config['script']}"
    local_script = f"/tmp/{job_config['script']}"
    
    return f"""
    gsutil cp {script_path} {local_script} && \
    python {local_script} \
        --project={config['project_id']} \
        --region={config['region']} \
        --runner=DataflowRunner \
        --job_name={job_config['job_name']}-$(date +%Y%m%d%H%M%S) \
        --temp_location={config['dataflow']['temp_location']} \
        --staging_location={config['dataflow']['staging_location']} \
        --input={input_path} \
        --output={output_table} \
        --service_account_email={config['dataflow']['service_account']} \
        --machine_type={config['dataflow']['machine_type']} \
        --max_num_workers={config['dataflow']['max_workers']}
    """

with DAG(
    dag_id='gcs_to_raw',
    default_args=default_args,
    description='Validate only GCS to Raw ingestion pipeline',
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['test', 'gcs-to-raw'],
    max_active_runs=1,
) as dag:

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

    wait_files >> gcs_to_raw
