CREATE TABLE IF NOT EXISTS test.requests
(
    runId             String,
    requestId         String,
    timestamp         DateTime64(3),
    requestDuration   Float64,
    httpCode          UInt16,
    bucketName        Nullable(String),
    objectKey         Nullable(String),
)
Engine = MergeTree()
ORDER BY (runId, timestamp)
