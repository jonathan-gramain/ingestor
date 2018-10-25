const async = require('async');
const http = require('http');
const AWS = require('aws-sdk');

const STATUS_BAR_LENGTH = 60;

function generateBody(size) {
    const randomString = Math.random().toString(36).slice(2);

    return new Array(Math.ceil(size / randomString.length))
        .fill(randomString)
        .join('')
        .slice(0, size);
}

function showStatus(values) {
    const statusBarTpl =
          new Array(STATUS_BAR_LENGTH).fill('=').concat(
              new Array(STATUS_BAR_LENGTH).fill(' ')).join('');
    const doneCount = values.successCount + values.errorCount;
    const completionRatio = doneCount / values.totalCount;
    const completeCharCount = Math.floor(completionRatio * STATUS_BAR_LENGTH);
    const statusBarTplOffset = STATUS_BAR_LENGTH - completeCharCount;
    process.stdout.write(
        '\r['
            + statusBarTpl.slice(statusBarTplOffset,
                                 statusBarTplOffset + STATUS_BAR_LENGTH)
            + `] ${doneCount}/${values.totalCount} `
            + `(${values.errorCount} errors)`);
}

function ingest(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }
    console.log(`
    endpoint: ${options.endpoint}
    prefix: "${options.prefix}"
    bucket: ${options.bucket}
    AWS profile: ${options.profile}
    workers: ${options.workers}
    object count: ${options.count}
    object size: ${options.size}
`);

    const credentials = new AWS.SharedIniFileCredentials({
        profile: options.profile,
    });
    const body = generateBody(options.size);
    const s3 = new AWS.S3({
        endpoint: options.endpoint,
        credentials,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        httpOptions: {
            agent: new http.Agent({ keepAlive: true }),
            timeout: 0,
        },
        maxRetries: 0,
    });
    let successCount = 0;
    let errorCount = 0;
    function updateStatusBar() {
        showStatus({
            successCount,
            errorCount,
            totalCount: options.count,
        });
    }
    const updateStatusBarInterval = setInterval(updateStatusBar, 200);
    async.timesLimit(options.count, options.workers, (n, next) => {
        const key = `${options.prefix}test-key-${`000000${n}`.slice(-6)}`;
        s3.putObject({
            Bucket: options.bucket,
            Key: key,
            Body: body,
        }, err => {
            if (err) {
                console.error(`error during "PUT ${options.bucket}/${key}":`,
                              err.message);
                ++errorCount;
            } else {
                ++successCount;
            }
            next();
        });
    }, () => {
        updateStatusBar();
        console.log();
        clearInterval(updateStatusBarInterval);
        cb();
    });
}

module.exports = ingest;
