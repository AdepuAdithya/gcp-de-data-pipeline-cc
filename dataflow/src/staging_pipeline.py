#Importing Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions as PO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime

#Configurations/Parameters
project ="gcp-de-batch-sim-464816"
region = "us-central1"
bucket = "gcp-de-batch-data-3"
raw_dataset = "Employee_Details_raw"
staging_dataset = "Employee_Details_stg"
raw_table   = "Department_raw"
staging_table   = "Department_stg"
input = f"{project}.{raw_dataset}.{raw_table}"
output = f"{project}.{staging_dataset}.{staging_table}"
temp_location = f"gs://{bucket}/temp"
staging_location = f"gs://{bucket}/staging"

def add_StagingIngestionTime(record):
    record['StagingIngestionTime'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    return record

# BigQuery Schema for Raw Layer
schema = {
    'fields': [
        {'name': 'DepartmentID', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'Name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'GroupName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ModifiedDate', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'RawIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'LoadDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'StagingIngestionTime', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ]
}

#Pipeline Configuration
def run():
    options = PO (
        runner='DataflowRunner',
        project=project,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        job_name='dep-staging-job',
        save_main_session=True
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=f'SELECT * FROM `{input}`', use_standard_sql=True)
            | 'Add Staging Ingestion Time' >> beam.Map(add_StagingIngestionTime)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=output,
                schema=schema,
                create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition= beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    run()