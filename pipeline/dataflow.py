import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json

# Kafka and BigQuery configuration
PROJECT_ID = 'your_project_id'
DATASET_ID = 'your_dataset'
TABLE_ID = 'nifty_50_stock_data'
KAFKA_TOPIC = 'nifty_stock_data'
KAFKA_SERVER = 'localhost:9092'  # Replace with your Kafka broker address

# Define the BigQuery table schema
table_schema = {
    'fields': [
        {'name': 'symbol', 'type': 'STRING'},
        {'name': 'date', 'type': 'DATE'},
        {'name': 'open', 'type': 'FLOAT'},
        {'name': 'high', 'type': 'FLOAT'},
        {'name': 'low', 'type': 'FLOAT'},
        {'name': 'close', 'type': 'FLOAT'},
        {'name': 'volume', 'type': 'INTEGER'}
    ]
}

# Pipeline options
class DataPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--runner', default='DataflowRunner')  # Change to 'DirectRunner' for local testing

pipeline_options = DataPipelineOptions(
    streaming=True,
    project=PROJECT_ID,
    region='us-central1',
    temp_location='gs://your-bucket/temp'
)
pipeline_options.view_as(StandardOptions).streaming = True

def parse_kafka_message(element):
    """Parse Kafka message from bytes to JSON."""
    message = element.value.decode('utf-8')
    return json.loads(message)

# Define the Apache Beam pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        # Read from Kafka topic
        | 'Read from Kafka' >> ReadFromKafka(
            consumer_config={'bootstrap.servers': KAFKA_SERVER},
            topics=[KAFKA_TOPIC]
        )
        # Parse JSON messages
        | 'Parse JSON' >> beam.Map(parse_kafka_message)
        # Write to BigQuery
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}",
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
