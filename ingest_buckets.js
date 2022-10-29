const batch = require('./batch');

function ingest_buckets(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-bucket-${new Date().toISOString().replace(/[:.]/g, '-').toLowerCase()}`;
    }

    const obj = batch.create(options);
    batch.showOptions(obj);

    console.log(`
`);

    const ingestOp = (s3, n, endSuccess, endError) => {
        const compMask = Buffer.alloc(6).fill('0').toString();
        const suffix = `${compMask}${n}`.slice(-compMask.length);
        const bucketName = `${options.prefix}-${suffix}`;
        s3.createBucket({
            Bucket: bucketName,
        }, err => {
            if (err) {
                console.error(`error during "PUT /${bucketName}":`,
                              err.message);
                return endError();
            }
            return endSuccess();
        });
    };
    batch.run(obj, ingestOp, cb);
}

module.exports = ingest_buckets;
