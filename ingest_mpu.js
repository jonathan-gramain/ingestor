const async = require('async');
const http = require('http');
const https = require('https');
const AWS = require('aws-sdk');

const STATUS_BAR_LENGTH = 60;
const STATUS_BAR_TPL =
      new Array(STATUS_BAR_LENGTH).fill('=').concat(
          new Array(STATUS_BAR_LENGTH).fill(' ')).join('');
const STATUS_UPDATE_PERIOD_MS = 200;
const STATS_PERIOD_MS = 2000;
const STATS_QUANTILES_WINDOW_SIZE = 1000;

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

const latenciesWindow = [];
let latenciesWindowPos = 0;

function queryStatsWindow() {
    return statsWindow[statsWindowIndex];
}

function updateStatsWindow(statsObj) {
    statsWindow[statsWindowIndex] = statsObj;
    statsWindowIndex = (statsWindowIndex + 1) % statsWindow.length;
}

function getLatencyQuantiles() {
    const sortedLatencies = latenciesWindow.concat().sort((a, b) => a - b);
    const quantileIndices = {
        lowest: 0,
        highest: sortedLatencies.length - 1,
        '10%': Math.floor(sortedLatencies.length / 10),
        '50%': Math.floor(sortedLatencies.length / 2),
        '90%': Math.floor(9 * sortedLatencies.length / 10),
    };
    if (sortedLatencies[0] === undefined) {
        return '';
    }
    return '|' + ['lowest', '10%', '50%', '90%', 'highest']
        .map(key => `${key} ${sortedLatencies[quantileIndices[key]]}ms`)
        .join('|') + '|';
}

function addLatency(latencyMs) {
    if (latenciesWindow.length < STATS_QUANTILES_WINDOW_SIZE) {
        latenciesWindow.push(latencyMs);
    } else {
        latenciesWindow[latenciesWindowPos] = latencyMs;
        latenciesWindowPos =
            (latenciesWindowPos + 1) % STATS_QUANTILES_WINDOW_SIZE;
    }
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
            + `${`        ${isNaN(kBPerSec) ? '' : kBPerSec.toFixed(0)}`.slice(-8)} KB/s `
            + getLatencyQuantiles() + '    ');
}

function ingest_mpu(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }
    console.log(`
    endpoint(s):  ${options.endpoint}
    prefix:       ${options.prefix}
    bucket:       ${options.bucket}
    workers:      ${options.workers}
    part count:   ${options.parts}
    part size:    ${options.size}
`);

    const credentials = new AWS.SharedIniFileCredentials({
        profile: options.profile,
    });
    const body = generateBody(options.size);
    const s3s = options.endpoint.map(endpoint => new AWS.S3({
        endpoint,
        credentials,
        s3ForcePathStyle: true,
        signatureVersion: 'v4',
        useHttps: endpoint.startsWith('https:'),
        httpOptions: {
            agent: endpoint.startsWith('https:') ?
                new https.Agent({ keepAlive: true }) :
                new http.Agent({ keepAlive: true }),
            timeout: 0,
        },
        maxRetries: 0,
    }));

    let successCount = 0;
    let errorCount = 0;
    function updateStatusBar() {
        showStatus({
            successCount,
            errorCount,
            totalParts: options.parts,
            partSize: options.size,
        });
    }
    const updateStatusBarInterval = setInterval(updateStatusBar,
                                                STATUS_UPDATE_PERIOD_MS);
    const key = `${options.prefix}test-key-mpu`;
    let uploadId;
    const partsInfo = [];
    async.waterfall([
        done => s3s[0].createMultipartUpload({
            Bucket: options.bucket,
            Key: key,
        }, (err, data) => {
            if (err) {
                console.error('error during createMultipartUpload for ' +
                              `${options.bucket}/${key}:`, err.message);
                return done(err);
            }
            return done(null, data);
        }),
        (data, done) => {
            uploadId = data.UploadId;
            async.timesLimit(options.parts, options.workers, (n, next) => {
                const startTime = Date.now();
                s3s[n % s3s.length].uploadPart({
                    Bucket: options.bucket,
                    Key: key,
                    UploadId: uploadId,
                    PartNumber: n + 1,
                    Body: body,
                }, (err, data) => {
                    if (err) {
                        console.error(`error during upload part for ` +
                                      `${options.bucket}/${key}:`,
                                      err.message);
                        ++errorCount;
                    } else {
                        ++successCount;
                        const endTime = Date.now();
                        addLatency(endTime - startTime);
                        partsInfo.push({
                            PartNumber: n + 1,
                            ETag: data.ETag,
                        });
                    }
                    return next();
                });
            }, err => done(err));
        },
        done => {
            partsInfo.sort((p1, p2) => p1.PartNumber < p2.PartNumber ? -1 : 1);
            if (!options.complete) {
                console.log('Key:', key);
                console.log('UploadId:', uploadId);
                console.log('Parts:', JSON.stringify({ Parts: partsInfo }));
                return done();
            }
            if (options.abort) {
                return s3s[0].abortMultipartUpload({
                    Bucket: options.bucket,
                    Key: key,
                    UploadId: uploadId,
                }, err => {
                    if (err) {
                        console.error(`error during abortMultipartUpload for ` +
                                      `${options.bucket}/${key}:`,
                                      err.message);
                    }
                    return done(err);
                });
            }
            s3s[0].completeMultipartUpload({
                Bucket: options.bucket,
                Key: key,
                UploadId: uploadId,
                MultipartUpload: {
                    Parts: partsInfo,
                },
            }, err => {
                if (err) {
                    console.error(`error during completeMultipartUpload for ` +
                                  `${options.bucket}/${key}:`,
                                  err.message);
                }
                return done(err);
            });
        },
    ], () => {
        updateStatusBar();
        console.log();
        clearInterval(updateStatusBarInterval);
        cb();
    });
}

module.exports = ingest_mpu;
