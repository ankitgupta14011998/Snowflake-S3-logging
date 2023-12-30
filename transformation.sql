-- To create the sql function to take input logs from stream and return the table with proper column. It uses python and regex to extract columns from string data.

create or replace function parse_s3_access_logs(log STRING)
returns table (
    bucketowner STRING,bucket_name STRING,requestdatetime STRING,remoteip STRING,requester STRING,
    requestid STRING,operation STRING,key STRING,request_uri STRING,httpstatus STRING,errorcode STRING,
    bytessent BIGINT,objectsize BIGINT,totaltime STRING,turnaroundtime STRING,referrer STRING, useragent STRING,
    versionid STRING,hostid STRING,sigv STRING,ciphersuite STRING,authtype STRING,endpoint STRING,tlsversion STRING)
language python
runtime_version=3.8
handler='S3AccessLogParser'
as $$
import re
class S3AccessLogParser:
    def clean(self,field):
        field = field.strip(' " " ')
        if field == '-':
            field = None
        return field
        
    def process(self, log):
        pattern = '([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$'
        lines = re.findall(pattern,log,re.M)
        for line in lines:
            yield(tuple(map(self.clean,line)))
$$;

-- This Select statement internally uses Inner join to get transformed data using the stage table and the sql function created above
select parsed_logs.*
    from s3_access_logs_staging
    join table(parse_s3_access_logs(s3_access_logs_staging.raw)) parsed_logs;

select * from s3_access_logs_staging;

-- Target table where transformed data wll be stored
create or replace table s3_access_logs(
 bucketowner STRING,bucket_name STRING,requestdatetime STRING,remoteip STRING,requester STRING,
    requestid STRING,operation STRING,key STRING,request_uri STRING,httpstatus STRING,errorcode STRING,
    bytessent BIGINT,objectsize BIGINT,totaltime STRING,turnaroundtime STRING,referrer STRING, useragent STRING,
    versionid STRING,hostid STRING,sigv STRING,ciphersuite STRING,authtype STRING,endpoint STRING,tlsversion STRING
);

-- Create task and provide the schedule time, to take data from stream every 10 mins and apply the transformation using the sql function and insert into the target table
create or replace task s3_access_logs_transformation
warehouse = security_quickstart
schedule = '10 minute'
when SYSTEM$STREAM_HAS_DATA('S3_ACCESS_LOGS_STREAM')
AS
INSERT INTO S3_ACCESS_LOGS
(select parsed_logs.*
    from s3_access_logs_stream
    join table(parse_s3_access_logs(s3_access_logs_stream.raw)) parsed_logs
    where s3_access_logs_stream.METADATA$ACTION = 'INSERT');

SELECT * FROM S3_ACCESS_LOGS;

ALTER TASK S3_ACCESS_LOGS_TRANSFORMATION RESUME;
ALTER TASK S3_ACCESS_LOGS_TRANSFORMATION SUSPEND;
ALTER TASK S3_aCCESS_LOGS_TRANSFORMATION 
SET SCHEDULE = '1 MINUTE'
