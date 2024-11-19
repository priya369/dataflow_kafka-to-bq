import base64
import datetime
import http.server
import json
import random
import google.auth
import google.auth.transport.urllib3
import urllib3
import functools
import time
import apache_beam as beam
from confluent_kafka import Consumer
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

import json

# Pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',   #for Dataflow job change to runner='DataflowRunner'
    project='valid-verbena-437709-h5',
    region='asia-south1',   #for Dataflow job change to 'us-west1'
    temp_location='gs://dataops-dataflow-2024/temp',
    staging_location='gs://dataops-dataflow-2024/staging',
    streaming=True,  #Enable streaming mode
    #Dataflow parameters that are optional
    job_name='streaming-kafka-bq-nifty50-v6',  #Set the Dataflow job name here
    num_workers=1,  #Specify the number of workers
    max_num_workers=10,  #Specify the maximum number of workers
    disk_size_gb=100,  #Specify the disk size in GB per worker
    autoscaling_algorithm='THROUGHPUT_BASED',  #Specify the autoscaling algorithm
    machine_type='n1-standard-4',  #Specify the machine type for the workers
    service_account_email='dataops-guru-sa@valid-verbena-437709-h5.iam.gserviceaccount.com'
)



# Define the pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:

    class ParseKafkaMessage(beam.DoFn):
        def process(self, element):
            # Decode the Kafka message (Key, Value) and parse JSON
             _, value = element
             message = json.loads(value.decode('utf-8'))
        
        # Ensure required fields are present
             if all(k in message for k in ['symbol', 'date', 'open', 'high', 'low', 'close', 'vwap']):
                yield {
                    'symbol': message['symbol'],
                    'date': message['date'],
                    'open': float(message['open']),
                    'high': float(message['high']),
                    'low': float(message['low']),
                    'close': float(message['close']),
                    'vwap': float(message['vwap'])
               }

    class TokenProvider:
        def __init__(self):
            self.credentials, _ = google.auth.default()
            self.http_client = urllib3.PoolManager()

        def valid_credentials(self):
            if not self.credentials.valid:
                self.credentials.refresh(google.auth.transport.urllib3.Request(self.http_client))
            return self.credentials

        def confluent_token(self):
            creds = self.valid_credentials()
            utc_expiry = creds.expiry.replace(tzinfo=datetime.timezone.utc)
            expiry_seconds = (utc_expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            return creds.token, time.time() + expiry_seconds

    def fetch_oauth_token():
       token_provider = TokenProvider()
       token, expiry_time = token_provider.confluent_token()
       return token
    
    # Pre-fetch the token
    oauth_token = fetch_oauth_token()
    kafka_cluster_name = 'dataopsguru-kafka'
    region = 'us-central1'
    project_id = 'valid-verbena-437709-h5'
    port = '9092'
    kafka_topic_name = 'dataops-kafka-topic'

    # Kafka Producer configuration with OAUTHBEARER authentication
    config = {
    'bootstrap.servers': f'bootstrap.{kafka_cluster_name}.{region}.managedkafka.{project_id}.cloud.goog:{port}',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_token,
    }


    PROJECT_ID = 'valid-verbena-437709-h5'
    DATASET_ID = 'nifty50'
    TABLE_ID = 'nifty50_historical_data_65'

    table_schema = {
    'fields': [
        {'name': 'symbol', 'type': 'STRING'},
        {'name': 'date', 'type': 'DATE'},
        {'name': 'open', 'type': 'FLOAT'},
        {'name': 'high', 'type': 'FLOAT'},
        {'name': 'low', 'type': 'FLOAT'},
        {'name': 'close', 'type': 'FLOAT'},
        {'name': 'vwap', 'type': 'FLOAT'},
        ] 
      }

    # Pipeline
    (
         pipeline
        # Read from Kafka topic
        | 'Read from Kafka' >> ReadFromKafka(
            consumer_config=config,
            topics=['dataops-kafka-topic'],
            with_metadata=False,
        )
        # Parse JSON messages
        | 'Parse JSON' >> beam.ParDo(ParseKafkaMessage())
        # Write to BigQuery
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}",
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
