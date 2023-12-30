-- Investigate who deleted which object
SELECT RequestDateTime, RemoteIP, Requester, Key 
FROM S3_ACCESS_LOGS 
WHERE key = 's3://s3-bucket-audit-stage-accesslogs/2023-12-29-10-16-19-43862272FFD6E668' AND operation like '%DELETE%';

-- IPs by number of requests
select count(*),REMOTEIP from s3_access_logs group by remoteip order by count(*) desc;

-- IPs by traffic
SELECT 
    remoteip,
    SUM(bytessent) AS uploadTotal,
    SUM(objectsize) AS downloadTotal,
    SUM(ZEROIFNULL(bytessent) + ZEROIFNULL(objectsize)) AS Total
FROM s3_access_logs
group by REMOTEIP
order by total desc;

-- Access denied errors
SELECT * FROM s3_access_logs WHERE httpstatus = '403';
-- All actions for a specific user
SELECT * 
FROM s3_access_logs_db.mybucket_logs 
WHERE requester='arn:aws:iam::123456789123:user/user_name';

-- Show anonymous requests
SELECT *
FROM s3_access_logs
WHERE Requester IS NULL;
