CREATE TABLE IF NOT EXISTS test.requests
(
    runId             String,
    requestId         String,
    timestamp         DateTime64(3),
    opType            String,
    requestDuration   Float64,
    httpCode          UInt16,
    bucketName        String,
    objectKey         String,
)
Engine = MergeTree()
ORDER BY (runId, timestamp)
