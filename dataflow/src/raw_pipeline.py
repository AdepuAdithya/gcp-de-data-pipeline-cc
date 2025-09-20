# Importing Libraries
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions as PO
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from datetime import datetime, date, timezone
from utils import load_json

# CSV File Parsing
def parsetxt(line, delimiter, schema_fields):
    fields = line.strip().split(delimiter)
    row = {}
    
    for i, field_schema in enumerate(schema_fields):

        field_name = field_schema['name']

        if field_name == 'RawIngestionTime':
            row[field_name] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        elif field_name == 'LoadDate':
            row[field_name] = date.today().strftime('%Y-%m-%d')
        else:
            row[field_name] = fields[i] if i < len(fields) and fields[i] != '' else None
    return row

# Pipeline Definition
def run(config_path):
    config = load_json(config_path)
    schema_fields = load_json(config['schema'])
    schema = {'fields': schema_fields}

    options = PO(
        runner=config.get('runner','DataflowRunner'),
        project=config.get('project'),
        region=config.get('region'),
        temp_location=config['temp_location'],
        staging_location=config['staging_location'],
        job_name=config.get('job_name', 'department-raw-ingest'),
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read TXT' >> beam.io.ReadFromText(config['source_path'], skip_header_lines=config.get('skip_header_lines', 1))
            | 'Parse TXT' >> beam.Map(parsetxt, delimiter=config['delimiter'], schema_fields=schema_fields)
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=config['target_table'],
                schema=schema,
                create_disposition=getattr(beam.io.BigQueryDisposition, config.get('create_disposition', 'CREATE_IF_NEEDED')),
                write_disposition=getattr(beam.io.BigQueryDisposition, config.get('write_disposition', 'WRITE_APPEND')),
            )
        )

if __name__ == '__main__':
    run()