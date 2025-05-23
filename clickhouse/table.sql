CREATE TABLE IF NOT EXISTS test.requests
(
    runId             String,
    requestId         String,
    workerId          String,
    timestamp         DateTime,
    requestDuration   Float64,
    httpCode          UInt16,
    accountName       Nullable(String),
    bucketName        Nullable(String),
    objectKey         Nullable(String),
)
Engine = MergeTree()
ORDER BY (runId, timestamp)
