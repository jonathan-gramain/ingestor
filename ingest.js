const async = require('async');
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
    add tags:            ${options.addTags ? 'yes' : 'no'}
    MPU parts:           ${options.mpuParts ? options.mpuParts : 'N/A'}
    random:              ${options.random ? 'yes' : 'no'}
    object lock:         ${options.objectLock ? 'enabled' : 'disabled'}
`);

    const extraPutOpts = {};
    if (options.objectLock) {
        // expire in one year from now
        const lockExpiration = new Date();
        lockExpiration.setYear(lockExpiration.getFullYear() + 1);
        extraPutOpts.ObjectLockMode = 'GOVERNANCE';
        extraPutOpts.ObjectLockRetainUntilDate = lockExpiration;
    }

    const putObject = (s3, bucket, key, body, tags, cb) => s3.putObject(Object.assign({
        Bucket: bucket,
        Key: key,
        Body: body,
        Tagging: tags,
    }, extraPutOpts), cb);

    const putMPU = (s3, bucket, key, body, tags, cb) => async.waterfall([
        next => s3.createMultipartUpload(Object.assign({
            Bucket: bucket,
            Key: key,
        }, extraPutOpts), (err, data) => {
            if (err) {
                console.error(`error during createMultipartUpload for ${bucket}/${key}:`, err.message);
                return next(err);
            }
            return next(null, data.UploadId);
        }),
        (uploadId, next) => async.timesLimit(options.mpuParts, 4, (n, partDone) => s3.uploadPart({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
            PartNumber: n + 1,
            Body: body[n],
        }, (err, partData) => {
            if (err) {
                console.error(`error during upload part for ${options.bucket}/${key}:`,
                              err.message);
                return partDone(err);
            }
            const partInfo = {
                PartNumber: n + 1,
                ETag: partData.ETag,
            };
            return partDone(null, partInfo);
        }), (err, partsInfo) => next(err, uploadId, partsInfo)),
        (uploadId, partsInfo, next) => {
            let repeat = 1;
            if (options.mpuFuzzRepeatCompleteProb) {
                while (Math.random() < options.mpuFuzzRepeatCompleteProb) {
                    repeat += 1;
                }
            }
            async.times(repeat, (i, completeDone) => s3.completeMultipartUpload({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: partsInfo,
                },
            }, completeDone), next);
        },
    ], err => {
        if (err) {
            console.error(`error during completeMultipartUpload for ${options.bucket}/${key}:`,
                          err.message);
        }
        return cb(err);
    });
    let body;
    let putFunc;
    if (options.mpuParts) {
        const partSize = Math.ceil(options.size / options.mpuParts);
        body = [];
        let remainingSize = options.size;
        while (remainingSize > 0) {
            body.push(generateBody(Math.min(partSize, remainingSize)));
            remainingSize -= partSize;
        }
        putFunc = putMPU;
    } else {
        body = generateBody(options.size);
        putFunc = putObject;
    }

    const ingestOp = (s3, n, objKey, endSuccess, endError) => {
        let tags = '';
        if (options.addTags) {
            const nTags = Math.floor(Math.random() * 50);
            const tagSet = [];
            for (let i = 1; i <= nTags; ++i) {
                tagSet.push(`TagKey${i}=VeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongTagValue${i}`);
            }
            tags = tagSet.join('&');
        }
        putFunc(s3, options.bucket, objKey, body, tags, err => {
            if (err) {
                console.error(`error during "PUT ${options.bucket}/${objKey}":`,
                              err.message);
                return endError();
            }
            if (!options.deleteAfterPut) {
                return endSuccess();
            }
            return s3.deleteObject({
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
        });
    };
    batch.init(obj, err => {
        if (err) {
            return cb(err);
        }
        return batch.run(obj, ingestOp, cb);
    });
}

module.exports = ingest;
