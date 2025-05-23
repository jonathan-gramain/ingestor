## Download clickhouse

cd clickhouse
curl https://clickhouse.com/ | sh

## Run the server

./clickhouse server

## Set up the database and table on the server

cat database.sql | curl 'http://localhost:8123/' --data-binary @-
cat table.sql | curl 'http://localhost:8123/' --data-binary @-

(optionally add a dummy run for testing)
cat req.sql | curl 'http://localhost:8123/' --data-binary @-

## (optional) Run a client

./clickhouse client

### Sample queries in clickhouse-client

```
fredmnl-L42-supervisor :) select runId, min(timestamp) as startTime, count() as requestCount from test.requests group by runId order by startTime

SELECT
    runId,
    min(timestamp) AS startTime,
    count() AS requestCount
FROM test.requests
GROUP BY runId
ORDER BY startTime ASC

Query id: 1da088f6-e5f6-4fba-ab70-ee4bff575798

   ┌─runId────────────────────────────┬───────────startTime─┬─requestCount─┐
1. │ test1                            │ 2023-10-01 00:00:00 │            3 │
2. │ 1d2a9b53fb0c70e83d68fd9bc8c2eb21 │ 2025-05-23 16:51:20 │            4 │
3. │ a7aa6cf76d2a9076a4054d2772ffecca │ 2025-05-23 16:58:15 │            9 │
   └──────────────────────────────────┴─────────────────────┴──────────────┘

3 rows in set. Elapsed: 0.055 sec.


fredmnl-L42-supervisor :) select * from test.requests where runId = 'a7aa6cf76d2a9076a4054d2772ffecca'

SELECT *
FROM test.requests
WHERE runId = 'a7aa6cf76d2a9076a4054d2772ffecca'

Query id: 94f6a28e-d2cb-43ae-9c58-e9b5f167beae

   ┌─runId────────────────────────────┬─requestId────────────────────────┬─workerId────┬───────────timestamp─┬─requestDuration─┬─httpCode─┬─accountName─┬─bucketName─┬─objectKey─┐
1. │ a7aa6cf76d2a9076a4054d2772ffecca │ f5569932ac37fa30c96d090aa4bff22b │ node-worker │ 2025-05-23 16:58:15 │           0.036 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
2. │ a7aa6cf76d2a9076a4054d2772ffecca │ c3d34aa28214a1291a828efa0040bcd9 │ node-worker │ 2025-05-23 16:58:15 │           0.025 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
3. │ a7aa6cf76d2a9076a4054d2772ffecca │ 97943f9b4d143abac9137fba82f8925b │ node-worker │ 2025-05-23 16:58:15 │            0.02 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
4. │ a7aa6cf76d2a9076a4054d2772ffecca │ b58add8dd34a67504cd9fe666b9ad150 │ node-worker │ 2025-05-23 16:58:15 │           0.023 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
5. │ a7aa6cf76d2a9076a4054d2772ffecca │ b3c43f1edaa0b0e048ea8354b77cfbba │ node-worker │ 2025-05-23 16:58:15 │           0.021 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
6. │ a7aa6cf76d2a9076a4054d2772ffecca │ d077d10944d13f0a4ef513b0cf369b70 │ node-worker │ 2025-05-23 16:58:15 │            0.02 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
7. │ a7aa6cf76d2a9076a4054d2772ffecca │ ded2e070e3efc4e233d7f4313829f81a │ node-worker │ 2025-05-23 16:58:15 │            0.02 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
8. │ a7aa6cf76d2a9076a4054d2772ffecca │ 5044f5c3659afb40532527adefc8b767 │ node-worker │ 2025-05-23 16:58:15 │            0.02 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
9. │ a7aa6cf76d2a9076a4054d2772ffecca │ bbc2079f38a9c80b9a214ae270ce0438 │ node-worker │ 2025-05-23 16:58:15 │           0.019 │      200 │ ᴺᵁᴸᴸ        │ fred2-1    │ dummy-key │
   └──────────────────────────────────┴──────────────────────────────────┴─────────────┴─────────────────────┴─────────────────┴──────────┴─────────────┴────────────┴───────────┘

9 rows in set. Elapsed: 0.004 sec.
```