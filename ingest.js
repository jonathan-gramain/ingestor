const batch = require('./batch');

function generateBody(size) {
    const randomString = Math.random().toString(36).slice(2);

    return new Array(Math.ceil(size / randomString.length))
        .fill(randomString)
        .join('')
        .slice(0, size);
}

function ingest(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }

    const obj = batch.create(options);
    batch.showOptions(obj);

    console.log(`
    one object:          ${options.oneObject ? 'yes' : 'no'}
    del after put:       ${options.deleteAfterPut ? 'yes' : 'no'}
`);

    const body = generateBody(options.size);

    const ingestOp = (s3, n, endSuccess, endError) => {
        let key;
        if (options.oneObject) {
            key = `${options.prefix}test-key`;
        } else {
            key = `${options.prefix}test-key-${`000000${n}`.slice(-6)}`;
        }
        s3.putObject({
            Bucket: options.bucket,
            Key: key,
            Body: body,
        }, err => {
            if (err) {
                console.error(`error during "PUT ${options.bucket}/${key}":`,
                              err.message);
                return endError();
            }
            if (!options.deleteAfterPut) {
                return endSuccess();
            }
            return s3.deleteObject({
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
    batch.run(obj, ingestOp, cb);
}

module.exports = ingest;
