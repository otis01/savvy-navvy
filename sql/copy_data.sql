
CREATE SCHEMA IF NOT EXISTS test_schema;

DROP TABLE IF EXISTS test_schema.geodata_processing;
CREATE TABLE test_schema.geodata_processing
(
    track_name      VARCHAR(100),
    id              BIGINT,
    latitude        FLOAT,
    longitude       FLOAT,
    coordinates     geometry,
    gpx_time        TIMESTAMP,
    speed           FLOAT,
    course          FLOAT,
    location_tag    VARCHAR(100),
    is_valid        BOOL
)
;

COPY test_schema.geodata_processing
  FROM 's3://my-test-bucket/geo-processing/manifest.json'
  IAM_ROLE 'arn:aws:iam::000000000000:role/redshift-s3-reader-all'
  REGION 'eu-west-1'
  DELIMITER '|'
  IGNOREHEADER 1
  EMPTYASNULL
  BLANKSASNULL
  CSV
  MANIFEST
;