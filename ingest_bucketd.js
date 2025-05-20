const async = require('async');
const bucketclient = require('bucketclient');

const batch = require('./batch');

const MD_TEMPLATE = Buffer.from(`{"owner-display-name":"test_1740598852","owner-id":"79704e05bbd2029a29a1ed79a1d30254db11f52e059ec7483ae1fe6abf19d7ce","content-length":1000,"content-type":"application/octet-stream","content-md5":"HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH","x-amz-version-id":"null","x-amz-server-version-id":"","x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"","x-amz-server-side-encryption-aws-kms-key-id":"","x-amz-server-side-encryption-customer-algorithm":"","x-amz-website-redirect-location":"","acl":{"Canned":"private","FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"","location":[{"key":"KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK","size":1000,"start":0,"dataStoreName":"site1","dataStoreType":"scality","dataStoreETag":"1:TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT"}],"isDeleteMarker":false,"tags":{},"replicationInfo":{"status":"","backends":[],"content":[],"destination":"","storageClass":"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":"site1","originOp":"s3:ObjectCreated:Put","last-modified":"2001-02-03T04:05:06.007Z","md-model-version":3}`, 'ascii');

const MD_TEMPLATE_MD5_OFFSET = MD_TEMPLATE.indexOf('HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH');
const MD_TEMPLATE_LOCATION_KEY_OFFSET = MD_TEMPLATE.indexOf('KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK');
const MD_TEMPLATE_ETAG_OFFSET = MD_TEMPLATE.indexOf('TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT');

const MD5_ZEROS = Buffer.from('00000000000000000000000000000000');
const LOCATION_ZEROS = Buffer.from('0000000000000000000000000000000000000000');

// The idea of this function is generating a body resembling real metadata. We
// replace the variable values by random contents looking like the actual
// values, because it can have a significant effect on database compression.
function generateBody() {
    // simulate content MD5 with a randomly generated 32-char hex string
    MD_TEMPLATE.set(MD5_ZEROS, MD_TEMPLATE_MD5_OFFSET);
    for (let i = 0; i < 4; ++i) {
        const md5part = Buffer.from(Math.floor(Math.random()*0x100000000).toString(16));
        MD_TEMPLATE.set(md5part, MD_TEMPLATE_MD5_OFFSET + 8*(i+1) - Buffer.byteLength(md5part));
    }
    // simulate location key with a randomly generated 40-char hex string
    MD_TEMPLATE.set(LOCATION_ZEROS, MD_TEMPLATE_LOCATION_KEY_OFFSET);
    for (let i = 0; i < 5; ++i) {
        const locationPart = Buffer.from(Math.floor(Math.random()*0x100000000).toString(16));
        MD_TEMPLATE.set(locationPart, MD_TEMPLATE_LOCATION_KEY_OFFSET + 8*(i+1)
                        - Buffer.byteLength(locationPart));
    }

    // etag is usually equal to md5 (except for MPUs)
    MD_TEMPLATE.set(MD_TEMPLATE.slice(MD_TEMPLATE_MD5_OFFSET, MD_TEMPLATE_MD5_OFFSET + 32),
                    MD_TEMPLATE_ETAG_OFFSET);

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

    const putObject = (bc, bucket, key, body, uids, cb) => {
        const params = {};
        if (options.versioned) {
            params.versioning = true;
        }
        bc.putObject(bucket, key, body, uids, cb, params);
    };

    const ingestOp = (bc, n, objKey, endSuccess, endError) => {
        const uids = n.toString();
        const body = generateBody();
        putObject(bc, options.bucket, objKey, body, uids, err => {
            if (err) {
                console.error(`error during "PUT ${options.bucket}/${objKey}":`,
                              err.message);
                return endError();
            }
            if (!options.deleteAfterPut) {
                return endSuccess();
            }
            return bc.deleteObject(options.bucket, objKey, uids, err => {
                if (err) {
                    console.error(`error during "DELETE ${options.bucket}/${objKey}":`,
                                  err.message);
                    return endError();
                }
                return endSuccess();
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
