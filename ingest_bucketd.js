const async = require('async');
const bucketclient = require('bucketclient');

const batch = require('./batch');

const MD_TEMPLATE = Buffer.from(`{"owner-display-name":"test_1740598852","owner-id":"79704e05bbd2029a29a1ed79a1d30254db11f52e059ec7483ae1fe6abf19d7ce","content-length":1000,"content-type":"application/octet-stream","content-md5":"HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH","x-amz-version-id":"null","x-amz-server-version-id":"","x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"","x-amz-server-side-encryption-aws-kms-key-id":"","x-amz-server-side-encryption-customer-algorithm":"","x-amz-website-redirect-location":"","acl":{"Canned":"private","FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"","location":[{"key":"KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK","size":1000,"start":0,"dataStoreName":"site1","dataStoreType":"scality","dataStoreETag":"1:TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"}],"isDeleteMarker":false,"tags":{},"replicationInfo":{"status":"","backends":[],"content":[],"destination":"","storageClass":"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":"site1","originOp":"s3:ObjectCreated:Put","last-modified":"2001-02-03T04:05:06.007Z","md-model-version":3}`, 'ascii');

const MD_TEMPLATE_MD5_OFFSET = MD_TEMPLATE.indexOf('HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH');
const MD_TEMPLATE_LOCATION_KEY_OFFSET = MD_TEMPLATE.indexOf('KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK');
const MD_TEMPLATE_ETAG_OFFSET = MD_TEMPLATE.indexOf('TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT');

function generateBody() {
    // simulate content MD5 with chars in the a-p range (for simplicity and performance)
    const md5 = Buffer.alloc(32).map(() => 97+Math.random()*16);
    MD_TEMPLATE.set(md5, MD_TEMPLATE_MD5_OFFSET);

    // simulate location key with chars in the A-P range (for simplicity and performance)
    const locKey = Buffer.alloc(40).map(() => 65+Math.random()*16);
    MD_TEMPLATE.set(locKey, MD_TEMPLATE_LOCATION_KEY_OFFSET);

    // etag is usually equal to md5 (except for MPUs)
    MD_TEMPLATE.set(md5, MD_TEMPLATE_ETAG_OFFSET);

    return MD_TEMPLATE.toString('ascii');
}

function ingest_bucketd(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }

    const bucketds = options.endpoint.map(endpoint => new bucketclient.RESTClient(endpoint));
    const batchObj = { options, s3s: bucketds };
    batch.showOptions(batchObj);

    console.log(`
    one object:          ${options.oneObject ? 'yes' : 'no'}
    del after put:       ${options.deleteAfterPut ? 'yes' : 'no'}
    random:              ${options.random ? 'yes' : 'no'}
`);

    const extraPutOpts = {};

    const putObject = (bc, bucket, key, body, uids, cb) => bc.putObject(bucket, key, body, uids, cb);

    const ingestOp = (bc, n, endSuccess, endError) => {
        batch.getKey(batchObj, n, key => {
            const uids = n.toString();
            const body = generateBody();
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
    batch.init(batchObj, err => {
        if (err) {
            return cb(err);
        }
        return batch.run(batchObj, ingestOp, cb);
    });
}

module.exports = ingest_bucketd;
