import os
from google.cloud import bigquery

def load_csv_to_bq(data, context):
        client = bigquery.Client()
        
        dataset_ref = client.dataset(os.environ['DATASET'])
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.schema = [
                bigquery.SchemaField('id', 'STRING'),
                bigquery.SchemaField('alias', 'STRING'),
                bigquery.SchemaField('name', 'STRING'),
                bigquery.SchemaField('categories', 'STRING'),
                bigquery.SchemaField('is_closed', 'BOOLEAN'),
                bigquery.SchemaField('review_count', 'INTEGER'),
                bigquery.SchemaField('rating', 'FLOAT'),
                bigquery.SchemaField('address', 'STRING'),
                bigquery.SchemaField('city', 'STRING'),
                bigquery.SchemaField('zip_code', 'INTEGER')
                ]
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV

        # get the URI for uploaded CSV in GCS from 'data'
        uri = 'gs://' + data['bucket'] + '/' + data['name']

        # load the data into BQ
        load_job = client.load_table_from_uri(
                uri,
                dataset_ref.table(os.environ['TABLE']),
                job_config=job_config)

        load_job.result()  # wait for table load to complete.
        print('Job finished.')