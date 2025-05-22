const batch = require('./batch');

function deleteall(options, cb) {
    const obj = batch.create(options);

    batch.showOptions(obj);

    console.log(`
    random:          ${options.random ? 'yes' : 'no'}
`);

    const deleteallOp = (s3, n, opType, objKey, endSuccess, endError) => {
        // opType ignored for now
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
