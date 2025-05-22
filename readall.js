const batch = require('./batch');

function readall(options, cb) {
    const obj = batch.create(options);

    batch.showOptions(obj);

    console.log(`
    random:          ${options.random ? 'yes' : 'no'}
`);

    const readallOp = (s3, n, opType, objKey, endSuccess, endError) => {
        // opType ignored for now
        s3.getObject({
            Bucket: options.bucket,
            Key: objKey,
        }, err => {
            if (err) {
                console.error(`error during "GET ${options.bucket}/${objKey}":`,
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
        batch.run(obj, readallOp, cb);
    });
}

module.exports = readall;
