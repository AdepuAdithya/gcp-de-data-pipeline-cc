from datetime import datetime, timedelta
from airflow import DAG
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
    dag_id='raw_to_staging_pipeline',
    default_args=default_args,
    description='Raw to Staging Data pipeline with Dataflow',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['raw-to-staging', 'dataflow'],
    max_active_runs=1,
) as dag:

    with TaskGroup('transform_to_staging', tooltip='Transform Raw to Staging tables') as raw_to_staging:
        
        employee_stg = BashOperator(
            task_id='employee_raw_to_staging',
            bash_command=build_dataflow_command(
                config['dataflow']['jobs']['raw_to_staging']['employee'],
                f"{config['project_id']}.{config['bigquery']['raw_dataset']}.{config['dataflow']['jobs']['gcs_to_raw']['employee']['table']}",
                f"{config['project_id']}.{config['bigquery']['staging_dataset']}.{config['dataflow']['jobs']['raw_to_staging']['employee']['table']}"
            ),
        )
        
        department_stg = BashOperator(
            task_id='department_raw_to_staging',
            bash_command=build_dataflow_command(
                config['dataflow']['jobs']['raw_to_staging']['department'],
                f"{config['project_id']}.{config['bigquery']['raw_dataset']}.{config['dataflow']['jobs']['gcs_to_raw']['department']['table']}",
                f"{config['project_id']}.{config['bigquery']['staging_dataset']}.{config['dataflow']['jobs']['raw_to_staging']['department']['table']}"
            ),
        )
