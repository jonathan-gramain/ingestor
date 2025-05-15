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

    const deleteallOp = (s3, n, endSuccess, endError) => {
        const idx = permuteIndex(n, options);
        batch.getKey(obj, n, key => {
            s3.deleteObject({
                Bucket: options.bucket,
                Key: key,
            }, err => {
                if (err) {
                    console.error(`error during "DELETE ${options.bucket}/${key}":`,
                                  err.message);
                    return endError();
                }
                return endSuccess();
            });
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
