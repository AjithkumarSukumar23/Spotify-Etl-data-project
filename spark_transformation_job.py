import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from pyspark.sql.functions import col, explode, to_date, concat_ws
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Load raw JSON data from S3
s3_path = "s3://spotify-project-ajith/raw_data/to_processed/"
source_df = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [s3_path]},
    format="json",
)
spotify_df = source_df.toDF()
spotify_df.show(5)

# Function to process albums
def process_albums(spotify_df):
    albums_df = spotify_df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("album_release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("album_url"),
    ).drop_duplicates(["album_id"])
    return albums_df

# Function to process artists
def process_artists(spotify_df):
    artists_df = spotify_df.withColumn("items", explode(col("items"))).select(
        col("items.track.artists").alias("artists"),
        col("items.track.id").alias("track_id"),
    ).withColumn("artist", explode(col("artists"))).select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("artist_url"),
        col("track_id"),
    ).drop_duplicates(["artist_id", "track_id"])
    return artists_df

# Function to process tracks
def process_tracks(spotify_df):
    tracks_df = spotify_df.withColumn("items", explode(col("items"))).withColumn(
        "artists", explode(col("items.track.artists"))
    ).select(
        col("items.track.id").alias("track_id"),
        col("items.track.name").alias("track_name"),
        col("items.track.duration_ms").alias("duration_ms"),
        col("items.track.popularity").alias("popularity"),
        col("items.track.external_urls.spotify").alias("track_url"),
        col("items.track.album.id").alias("album_id"),
        col("artists.id").alias("artist_id"),
        col("items.added_at").alias("song_added_at"),
    ).drop_duplicates(["track_id"])
    return tracks_df

# Process data
album_df = process_albums(spotify_df)
album_df.show(5)

artist_df = process_artists(spotify_df)
artist_df.show(5)

track_df = process_tracks(spotify_df)
track_df.show(5)

# Function to write DataFrame to S3
def write_to_s3(spotify_df, path_suffix, format_type="csv"):
    try:
        # Convert DataFrame to DynamicFrame
        dynamic_frame = DynamicFrame.fromDF(spotify_df, glueContext, "dynamic_frame")

        # Write DynamicFrame to S3
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": f"s3://spotify-project-ajith/transformed_data/{path_suffix}/"
            },
            format=format_type,
            format_options={
                "separator": ",",
                "quoteChar": '"',
                "withHeader": True,
            },
        )
        print(f"Data successfully written to s3://spotify-project-ajith/transformed_data/{path_suffix}/")
    except Exception as e:
        print(f"Error writing to S3: {str(e)}")

write_to_s3(album_df, f"album_data/album_transformed_data_{datetime.now().strftime('%Y-%m-%d')}", "csv")
write_to_s3(artist_df, f"artist_data/artist_transformed_data_{datetime.now().strftime('%Y-%m-%d')}", "csv")
write_to_s3(track_df, f"songs_data/song_transformed_data_{datetime.now().strftime('%Y-%m-%d')}", "csv")

def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        keys = [content['Key'] for content in response['Contents'] if content['Key'].endswith('.json')]
        return keys
    return []

bucket_name = "spotify-project-ajith"
prefix = "raw_data/to_processed"
spotify_keys = list_s3_objects(bucket_name, prefix)

def move_and_delete_files(spotify_keys, bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': bucket,
            'Key': key,
        }
        destination_key = f'raw_data/processed/{key.split("/")[-1]}'
        try:
            s3_resource.meta.client.copy(copy_source, bucket, destination_key)
            s3_resource.Object(bucket, key).delete()
        except Exception as e:
            print(f"Error processing {key}: {str(e)}")

move_and_delete_files(spotify_keys, bucket_name)
job.commit()
