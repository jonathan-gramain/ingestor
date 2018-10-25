const async = require('async');
const http = require('http');
const AWS = require('aws-sdk');

const STATUS_BAR_LENGTH = 60;
const STATUS_BAR_TPL =
      new Array(STATUS_BAR_LENGTH).fill('=').concat(
          new Array(STATUS_BAR_LENGTH).fill(' ')).join('');
const STATUS_UPDATE_PERIOD_MS = 200;
const STATS_PERIOD_MS = 2000;

function generateBody(size) {
    const randomString = Math.random().toString(36).slice(2);

    return new Array(Math.ceil(size / randomString.length))
        .fill(randomString)
        .join('')
        .slice(0, size);
}

const statsWindow = new Array(Math.max(
    Math.floor(STATS_PERIOD_MS / STATUS_UPDATE_PERIOD_MS),
    1)).fill({});
let statsWindowIndex = 0;

function queryStatsWindow() {
    return statsWindow[statsWindowIndex];
}

function updateStatsWindow(statsObj) {
    statsWindow[statsWindowIndex] = statsObj;
    statsWindowIndex = (statsWindowIndex + 1) % statsWindow.length;
}

function showStatus(values) {
    const doneCount = values.successCount + values.errorCount;
    const completionRatio = doneCount / values.totalCount;
    const completeCharCount = Math.floor(completionRatio * STATUS_BAR_LENGTH);
    const statusBarTplOffset = STATUS_BAR_LENGTH - completeCharCount;
    const doneCountInPeriod = doneCount - queryStatsWindow().doneCount;
    updateStatsWindow({ doneCount });
    const opsPerSec = (doneCountInPeriod * 1000) / STATS_PERIOD_MS;
    const kBPerSec = (doneCountInPeriod * values.objectSize) / STATS_PERIOD_MS;
    process.stdout.write(
        '\r['
            + STATUS_BAR_TPL.slice(statusBarTplOffset,
                                   statusBarTplOffset + STATUS_BAR_LENGTH)
            + `] `
            + `   ${Math.floor(doneCount / values.totalCount * 100)}`.slice(-3)
            + `% ` + `        ${doneCount}`.slice(-8)
            + ` ops (${values.errorCount} errors) `
            + `${`      ${isNaN(opsPerSec) ? '' : opsPerSec.toFixed(0)}`.slice(-6)} op/s `
            + `${`        ${isNaN(kBPerSec) ? '' : kBPerSec.toFixed(0)}`.slice(-8)} KB/s`);
}

function ingest(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }
    console.log(`
    endpoint:     ${options.endpoint}
    prefix:       ${options.prefix}
    bucket:       ${options.bucket}
    workers:      ${options.workers}
    object count: ${options.count}
    object size:  ${options.size}
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
            objectSize: options.size,
        });
    }
    const updateStatusBarInterval = setInterval(updateStatusBar,
                                                STATUS_UPDATE_PERIOD_MS);
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
