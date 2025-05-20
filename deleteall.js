const batch = require('./batch');

function permuteIndex(n, options) {
    if (options.random) {
        // multiply by a prime number to have a somewhat randomized
        // bijective mapping and read all objects exactly once
        return (n * 5776357) % options.count;
    }
    return n;
}

function deleteall(options, cb) {
    const obj = batch.create(options);

    batch.showOptions(obj);

    console.log(`
    random:          ${options.random ? 'yes' : 'no'}
`);

    const deleteallOp = (s3, n, objKey, endSuccess, endError) => {
        s3.deleteObject({
            Bucket: options.bucket,
            Key: objKey,
        }, err => {
            if (err) {
                console.error(`error during "DELETE ${options.bucket}/${objKey}":`,
                              err.message);
                return endError();
            }
            return endSuccess();
        });
    };
    batch.init(obj, err => {
        if (err) {
            return cb(err);
        }
        batch.run(obj, deleteallOp, cb);
    });
}

module.exports = deleteall;
