const async = require('async');
const batch = require('./batch');

const LISTING_LIMIT = 1000;

function generateBody(size) {
    const randomString = Math.random().toString(36).slice(2);

    return new Array(Math.ceil(size / randomString.length))
        .fill(randomString)
        .join('')
        .slice(0, size);
}

function permuteIndex(n, options) {
    if (options.random) {
        // multiply by a prime number to have a somewhat randomized
        // bijective mapping and read all objects exactly once
        return (n * 5776357) % options.count;
    }
    return n;
}

function deleteversions(options, cb) {
    const obj = batch.create(options);
    const allKeys = [];

    batch.showOptions(obj);

    console.log(`
    random:          ${options.random ? 'yes' : 'no'}
`);

    const deleteversionsOp = (s3, n, endSuccess, endError) => {
        const idx = permuteIndex(n, options);
        const { Key, VersionId } = allKeys[idx];
        s3.deleteObject({
            Bucket: options.bucket,
            Key,
            VersionId,
        }, err => {
            if (err) {
                console.error(`error during "DELETE ${options.bucket}/${Key}?versionId=${VersionId}":`,
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
        const { s3, options } = obj;
        let KeyMarker = null;
        let VersionIdMarker = null;
        console.log('listing object versions');
        async.doWhilst(
            done => s3.listObjectVersions({
                Bucket: options.bucket,
                Prefix: options.prefix || undefined,
                MaxKeys: LISTING_LIMIT,
                KeyMarker,
                VersionIdMarker,
            }, (err, result) => {
                if (err) {
                    return done(err);
                }
                allKeys.push(...result.Versions.map(({ Key, VersionId }) => ({ Key, VersionId })));
                allKeys.push(...result.DeleteMarkers.map(({ Key, VersionId }) => ({ Key, VersionId })));
                KeyMarker = result.NextKeyMarker;
                VersionIdMarker = result.NextVersionIdMarker;
                done();
            }),
            () => !!(KeyMarker || VersionIdMarker),
            err => {
                if (err) {
                    return cb(err);
                }
                console.log(`starting batch with ${allKeys.length} keys`);
                options.count = allKeys.length;
                options.prefix = '';
                batch.run(obj, deleteversionsOp, cb);
            }
        );
    });
}

module.exports = deleteversions;
