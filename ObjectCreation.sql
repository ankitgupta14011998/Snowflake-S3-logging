-- Create Storage Integration to authenticate everytime new data has to load from S3 bucket

CREATE OR REPLACE STORAGE INTEGRATION S3_INT_S3_ACCESS_LOGS
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::980872034360:role/s3_snowflake_access_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://s3-bucket-audit-stage-accesslogs/');

DESC STORAGE INTEGRATION S3_INT_S3_ACCESS_LOGS;

-- Describe Storage Integration and take below 2 properties

STORAGE_AWS_IAM_USER_ARN : 'arn:aws:iam::710762137437:user/4zrh0000-s'
STORAGE_AWS_EXTERNAL_ID : 'LP98491_SFCRole=2_3sanh268WllHuKOy2/IFCgwUCig='

  -- Optional : export these variables in S3 cloudshell, so that we can use these variables to parametrize our policy and roles in AWS
export BUCKET_NAME='s3-bucket-audit-stage-accesslogs'
export PREFIX='' 
export ROLE_NAME='s3_snowflake_access_role'
export STORAGE_AWS_IAM_USER_ARN='arn:aws:iam::710762137437:user/4zrh0000-s'
export STORAGE_AWS_EXTERNAL_ID='LP98491_SFCRole=2_3sanh268WllHuKOy2/IFCgwUCig=';

--Create warehouse
CREATE WAREHOUSE security_quickstart with 
warehouse_size = medium
auto_suspend=60;

 --Create file_format

CREATE FILE FORMAT IF NOT EXISTS TEXT_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = NONE
SKIP_BLANK_LINES = TRUE
ESCAPE_UNENCLOSED_FIELD = NONE;

--Create Stage and use the storage_integration and the location of s3 bucket
CREATE STAGE S3_ACCESS_LOGS
URL = 's3://s3-bucket-audit-stage-accesslogs/'
STORAGE_INTEGRATION = S3_INT_S3_ACCESS_LOGS;

--To get the data present in stage
list @s3_access_logs;

--Create the staging table to store the data from stage
CREATE TABLE S3_ACCESS_LOGS_STAGING(
RAW TEXT,
TIMESTAMP DATETIME);

--Create stream over the staging table to track any change happened in table
CREATE STREAM S3_ACCESS_LOGS_STREAM ON TABLE S3_ACCESS_LOGS_STAGING;

--Copy command to copy data from stage to stage table
COPY INTO S3_ACCESS_LOGS_STAGING
FROM( SELECT $1,CURRENT_TIMESTAMP() AS TIMESTAMP FROM @S3_ACCESS_LOGS (FILE_FORMAT => TEXT_FORMAT));

SELECT * FROM S3_ACCESS_LOGS_STAGING;

-- Create snowpipe to store the copy command and auto ingest ingest it to table as soon as data is available in stage
CREATE PIPE S3_ACCESS_LOGS_PIPE AUTO_INGEST = TRUE AS 
COPY INTO S3_ACCESS_LOGS_STAGING
FROM( SELECT $1,CURRENT_TIMESTAMP() AS TIMESTAMP FROM @S3_ACCESS_LOGS (FILE_FORMAT => TEXT_FORMAT))
;

SHOW PIPES; S3_ACCESS_LOGS_PIPE

  -- extract the notification channel from pipe and create SQS notification in S3 bucket(to get notification to snowpipe as soon as there is any change in S3 bucket)
NOTIFICATION_CHANNEL : 'arn:aws:sqs:ap-south-1:710762137437:sf-snowpipe-AIDA2K7FZQ5OXPYVITPNJ-G8NKxqfFaDMDzE47_yR0rg';

select * from S3_ACCESS_LOGS_STREAM;

-- to refresh the pipe if there is any new data in stage
alter pipe s3_Access_logs_pipe refresh;

SELECT * fROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
DATE_RANGE_START=>DATEADD('DAY',-14,CURRENT_DATE()),
        DATE_RANGE_END=> CURRENT_DATE(),
        PIPE_NAME=>'PUBLIC.S3_ACCESS_LOGS_PIPE'));

SELECT * FROM INFORMATION_SCHEMA.LOAD_HISTORY
WHERE TABLE_NAME = 'S3_ACCESS_LOGS_STAGING';
