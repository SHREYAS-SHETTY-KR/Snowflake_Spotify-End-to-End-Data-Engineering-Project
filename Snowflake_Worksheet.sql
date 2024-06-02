-- Create the album table in Snowflake
CREATE OR REPLACE TABLE album (
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);

-- Create the artist table in Snowflake
CREATE OR REPLACE TABLE artist (
    artist_id STRING,
    artist_name STRING,
    external_url STRING
);

-- Create the song table in Snowflake
CREATE OR REPLACE TABLE song (
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added TIMESTAMP_NTZ,
    album_id STRING,
    artist_id STRING
);

-- Create a storage integration to connect Snowflake with AWS S3
CREATE OR REPLACE STORAGE INTEGRATION S3_CONN
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::837934442683:role/snowflake_s3_conn'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-project-shrey')
COMMENT = 'Creating connection to S3';

-- Describe the created storage integration to verify its details
DESC INTEGRATION S3_CONN;

-- Create a file format in Snowflake for CSV files
CREATE OR REPLACE FILE FORMAT SPOTIFY.SPOTY.CSV_FILE_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE;

-- Creating stage and Snowpipe for the album table
CREATE OR REPLACE STAGE SPOTIFY.SPOTY.S3_ALBUM
URL = 's3://snowflake-project-shrey/transformed_data/album_data/'
STORAGE_INTEGRATION = S3_CONN
FILE_FORMAT = SPOTIFY.SPOTY.CSV_FILE_FORMAT;

-- List the files in the album stage to verify its connection
LIST @SPOTIFY.SPOTY.S3_ALBUM;

-- Create a Snowpipe for automatic ingestion of album data
CREATE OR REPLACE PIPE SPOTIFY.SPOTY.ALBUM
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY.SPOTY.ALBUM
FROM @SPOTIFY.SPOTY.S3_ALBUM
ON_ERROR = 'CONTINUE';

-- Describe the created Snowpipe for the album table
DESC PIPE SPOTIFY.SPOTY.ALBUM;

-- Creating stage and Snowpipe for the artist table
CREATE OR REPLACE STAGE SPOTIFY.SPOTY.S3_ARTIST
URL = 's3://snowflake-project-shrey/transformed_data/artist_data/'
STORAGE_INTEGRATION = S3_CONN
FILE_FORMAT = SPOTIFY.SPOTY.CSV_FILE_FORMAT;

-- List the files in the artist stage to verify its connection
LIST @SPOTIFY.SPOTY.S3_ARTIST;

-- Create a Snowpipe for automatic ingestion of artist data
CREATE OR REPLACE PIPE SPOTIFY.SPOTY.ARTIST
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY.SPOTY.ARTIST
FROM @SPOTIFY.SPOTY.S3_ARTIST
ON_ERROR = 'CONTINUE';

-- Describe the created Snowpipe for the artist table
DESC PIPE SPOTIFY.SPOTY.ARTIST;

-- Creating stage and Snowpipe for the song table
CREATE OR REPLACE STAGE SPOTIFY.SPOTY.S3_SONG
URL = 's3://snowflake-project-shrey/transformed_data/songs_data/'
STORAGE_INTEGRATION = S3_CONN
FILE_FORMAT = SPOTIFY.SPOTY.CSV_FILE_FORMAT;

-- List the files in the song stage to verify its connection
LIST @SPOTIFY.SPOTY.S3_SONG;

-- Create a Snowpipe for automatic ingestion of song data
CREATE OR REPLACE PIPE SPOTIFY.SPOTY.SONG
AUTO_INGEST = TRUE
AS
COPY INTO SPOTIFY.SPOTY.SONG
FROM @SPOTIFY.SPOTY.S3_SONG
ON_ERROR = 'CONTINUE';

-- Describe the created Snowpipe for the song table
DESC PIPE SPOTIFY.SPOTY.SONG;

-- Select all records from the album table
SELECT * FROM ALBUM;

-- Select all records from the song table
SELECT * FROM SONG;

-- Select all records from the artist table
SELECT * FROM ARTIST;

