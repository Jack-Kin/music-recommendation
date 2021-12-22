!pip install -q -U apache-beam[gcp]
import IPython
app = IPython.Application.instance()
app.kernel.do_shutdown(True)
import os
from datetime import datetime
import apache_beam as beam
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore

PROJECT_ID = 'bigdatat-6893'
BUCKET = 'music-recommendation'
BQ_REGION = 'us-east1'
DF_REGION = 'us-east2'
BQ_DATASET_NAME = 'recommendations'
BQ_TABLE_NAME = 'playlist'
DS_KIND = 'song'

!gcloud config set project $PROJECT_ID

!bq mk --dataset \
    --location={BQ_REGION} \
    --project_id={PROJECT_ID} \
    --headless=True \
    {PROJECT_ID}:{BQ_DATASET_NAME}

# define the pipeline
def run_copy_bq_data_pipeline(args):

  schema = 'list_Id:INT64, track_Id:INT64, track_title:STRING, track_artist:STRING'

  query = '''
    SELECT 
      id list_Id, 
      tracks_data_id track_Id, 
      tracks_data_title track_title,
      tracks_data_artist_name track_artist
    FROM `bigquery-samples.playlists.playlist`
    WHERE tracks_data_title IS NOT NULL AND tracks_data_id > 0
    GROUP BY list_Id, track_Id, track_title, track_artist;
  '''

  pipeline_options = beam.options.pipeline_options.PipelineOptions(**args)
  with beam.Pipeline(options=pipeline_options) as pipeline:

    _ = (
        pipeline
        | 'ReadFromBigQuery' >> beam.io.Read(beam.io.BigQuerySource(
            project=PROJECT_ID, query=query, use_standard_sql=True))
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=BQ_TABLE_NAME, dataset=BQ_DATASET_NAME, project=PROJECT_ID,
            schema=schema,
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_TRUNCATE'
        )
    )

# run the pipeline
DATASET = 'playlist'
RUNNER = 'DataflowRunner'

job_name = f'copy-bigquery-{datetime.utcnow().strftime("%y%m%d%H%M%S")}'

args = {
    'job_name': job_name,
    'runner': RUNNER,
    'project': PROJECT_ID,
    'temp_location': f'gs://{BUCKET}/dataflow_tmp',
    'region': DF_REGION
}

print("Pipeline args are set.")

print("Running pipeline...")
%time run_copy_bq_data_pipeline(args)
print("Pipeline is done.")


%%bigquery  --project $PROJECT_ID

CREATE OR REPLACE VIEW `recommendations.vw_item_groups`
AS
SELECT
  list_Id AS group_Id,
  track_Id AS item_Id
FROM
  `recommendations.playlist`


# export song info to Datastore
def create_entity(song_info, kind):

  from apache_beam.io.gcp.datastore.v1new.types import Entity
  from apache_beam.io.gcp.datastore.v1new.types import Key

  track_Id = song_info.pop("track_Id")
  key = Key([kind, track_Id])
  song_entity = Entity(key)
  song_entity.set_properties(song_info)
  return song_entity

def run_export_to_datatore_pipeline(args):

    query = f'''
      SELECT  
        track_Id, 
        MAX(track_title) track_title, 
        MAX(track_artist) artist
      FROM 
        `{BQ_DATASET_NAME}.{BQ_TABLE_NAME}`
      GROUP BY track_Id
    '''

    pipeline_options = beam.options.pipeline_options.PipelineOptions(**args)
    with beam.Pipeline(options=pipeline_options) as pipeline:

      _ = (
        pipeline
        | 'ReadFromBigQuery' >> beam.io.Read(beam.io.BigQuerySource(
            project=PROJECT_ID, query=query, use_standard_sql=True))
        | 'ConvertToDatastoreEntity' >> beam.Map(create_entity, DS_KIND)
        | 'WriteToDatastore' >> WriteToDatastore(project=PROJECT_ID)
      )

# run the Datastore pipeline
import os
from datetime import datetime

DATASET = 'playlist'
RUNNER = 'DataflowRunner'

job_name = f'load-datastore-{datetime.utcnow().strftime("%y%m%d%H%M%S")}'

args = {
    'job_name': job_name,
    'runner': RUNNER,
    'project': PROJECT_ID,
    'temp_location': f'gs://{BUCKET}/dataflow_tmp',
    'region': DF_REGION
}

print("Pipeline args are set.")

print("Running pipeline...")
%time run_export_to_datatore_pipeline(args)
print("Pipeline is done.")