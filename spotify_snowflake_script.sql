--- created database
create database spotify_db;

--creating storage integration
create or replace storage integration s3_init
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::863518427281:role/spotify-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS=('s3://spotify-project-ajith')
  comment ='Creating connection to S3 ';


  --verifying integration details
DESC integration s3_init;


  CREATE OR REPLACE file format csv_fileformat
      type=csv
      field_delimiter=','
      skip_header=1
      null_if=('NULL','null')
      empty_field_as_null=TRUE;



     CREATE OR REPLACE stage spotify_stage
   URL='s3://spotify-project-ajith/transformed_data/'
   STORAGE_INTEGRATION=s3_init
   FILE_FORMAT=csv_fileformat;


   list @spotify_stage;

   --creating tables
 CREATE OR REPLACE TABLE tbl_album(
     album_id STRING,
     album_name STRING,
     album_release_date DATE,
     total_tracks INT,
     album_url STRING
      )

       CREATE OR REPLACE TABLE tbl_artists(
      artist_id STRING,
      artist_name STRING,
      artist_url STRING,
      track_id String
      )

      CREATE OR REPLACE TABLE tbl_songs(
     track_id STRING,
     track_name STRING,
     duration_ms INT,
     popularity INT,
     track_url STRING,
     album_id STRING,
     artist_id STRING,
     song_added_at DATE
  )

    COPY INTO tbl_album
  FROM @spotify_stage/album_data/album_transformed_data_2025-01-04/run-1735978679916-part-r-00000;

  select count (*) from tbl_album

      COPY INTO tbl_artists
  FROM @spotify_stage/artist_data/artist_transfomeddata_2025-01-02/run-1735850338142-part-r-00000;

  select count (*) from tbl_artists

        COPY INTO tbl_songs
  FROM @spotify_stage/songs_data/song_transformed_data_2025-01-04/run-1735978691279-part-r-00000;


   select count (*) from tbl_songs

--- creating snowpipe
   CREATE OR REPLACE SCHEMA PIPE;


CREATE OR REPLACE PIPE spotify_db.pipe.tbl_album_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data/;

CREATE OR REPLACE PIPE spotify_db.pipe.tbl_artists_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist_data/;

CREATE OR REPLACE PIPE spotify_db.pipe.tbl_songs_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs_data/;


--Event creation

DESC pipe pipe.tbl_album_pipe

DESC pipe pipe.tbl_artists_pipe

DESC pipe pipe.tbl_songs_pipe

