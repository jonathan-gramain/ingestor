const async = require('async');
const bucketclient = require('bucketclient');

const batch = require('./batch');

const OVERHEAD_START = Buffer.from('{"attr":"');
const OVERHEAD_END = Buffer.from('"}');
const OVERHEAD_SIZE = OVERHEAD_START.byteLength + OVERHEAD_END.byteLength;

function generateBody(size) {
    const resSize = Math.max(size, OVERHEAD_SIZE);
    const res = Buffer.alloc(resSize).map(() => 35+Math.random()*54);
    res.set(OVERHEAD_START, 0);
    res.set(OVERHEAD_END, res.byteLength - OVERHEAD_END.byteLength);
    return res.toString('ascii');
}

function ingest_bucketd(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }

    const bucketds = options.endpoint.map(endpoint => new bucketclient.RESTClient(endpoint));
    batch.showOptions({ options });

    console.log(`
    one object:          ${options.oneObject ? 'yes' : 'no'}
    del after put:       ${options.deleteAfterPut ? 'yes' : 'no'}
    random:              ${options.random ? 'yes' : 'no'}
`);

    const extraPutOpts = {};

    const putObject = (bc, bucket, key, body, uids, cb) => bc.putObject(bucket, key, body, uids, cb);

    const ingestOp = (bc, n, endSuccess, endError) => {
        batch.getKey({ options }, n, key => {
            const uids = n.toString();
            const body = generateBody(options.size);
            putObject(bc, options.bucket, key, body, uids, err => {
                if (err) {
                    console.error(`error during "PUT ${options.bucket}/${key}":`,
                                  err.message);
                    return endError();
                }
                if (!options.deleteAfterPut) {
                    return endSuccess();
                }
                return bc.deleteObject(options.bucket, key, uids, err => {
                    if (err) {
                        console.error(`error during "DELETE ${options.bucket}/${key}":`,
                                      err.message);
                        return endError();
                    }
                    return endSuccess();
                });
            });
        });
    };
    batch.init({ options }, err => {
        if (err) {
            return cb(err);
        }
        return batch.run({ options, s3s: bucketds }, ingestOp, cb);
    });
}

module.exports = ingest_bucketd;
