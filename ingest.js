const async = require('async');
const http = require('http');
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
    one object:   ${options.oneObject ? 'yes' : 'no'}
    del after put:${options.deleteAfterPut ? 'yes' : 'no'}
    rate limit:   ${options.rateLimit ? `${options.rateLimit} op/s` : 'none'}
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
    let nextTime = options.rateLimit ? Date.now() : null;
    async.timesLimit(options.count, options.workers, (n, next) => {
        let key;
        if (options.oneObject) {
            key = `${options.prefix}test-key`;
        } else {
            key = `${options.prefix}test-key-${`000000${n}`.slice(-6)}`;
        }
        const startTime = Date.now();
        const doOp = () => {
            const opStartTime = Date.now();
            const endOp = () => {
                ++successCount;
                const endTime = Date.now();
                addLatency(endTime - opStartTime);
                next();
            };
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
                    if (options.deleteAfterPut) {
                        s3.deleteObject({
                            Bucket: options.bucket,
                            Key: key,
                        }, err => {
                            if (err) {
                                console.error(`error during "DELETE ${options.bucket}/${key}":`,
                                              err.message);
                                ++errorCount;
                            } else {
                                endOp();
                            }
                        });
                    } else {
                        endOp();
                    }
                }
            });
        };
        if (nextTime) {
            if (startTime > nextTime) {
                doOp();
            } else {
                setTimeout(doOp, nextTime - startTime);
            }
            const nextDelay = 1000 / options.rateLimit;
            nextTime += nextDelay;
            if (nextTime < startTime) {
                // we're lagging behind the rate limit, keep up to
                // resynchronize
                nextTime = startTime;
            }
        } else {
            doOp();
        }
    }, () => {
        updateStatusBar();
        console.log();
        clearInterval(updateStatusBarInterval);
        cb();
    });
}

module.exports = ingest;
