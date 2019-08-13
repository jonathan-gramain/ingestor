const async = require('async');
const http = require('http');
const fs = require('fs');
const AWS = require('aws-sdk');

const STATUS_BAR_LENGTH = 60;
const STATUS_BAR_TPL =
      new Array(STATUS_BAR_LENGTH).fill('=').concat(
          new Array(STATUS_BAR_LENGTH).fill(' ')).join('');
const STATUS_UPDATE_PERIOD_MS = 200;
const STATS_PERIOD_MS = 2000;
const STATS_QUANTILES_WINDOW_SIZE = 1000;

const LATENCY_QUANTILES = {
    'lowest': 0,
    '10%': 0.1,
    '50%': 0.5,
    '90%': 0.9,
    'highest': 0.9999999,
};

const LATENCY_QUANTILES_LABELS = Object.entries(LATENCY_QUANTILES)
      .sort((q1, q2) => q1[1] < q2[1] ? -1 : 1)
      .map(q => q[0]);

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

let csvStatsFile;

function queryStatsWindow() {
    return statsWindow[statsWindowIndex];
}

function updateStatsWindow(statsObj) {
    statsWindow[statsWindowIndex] = statsObj;
    statsWindowIndex = (statsWindowIndex + 1) % statsWindow.length;
}

function getLatencyQuantiles() {
    const sortedLatencies = latenciesWindow.concat().sort((a, b) => a - b);
    const quantiles = {};
    if (sortedLatencies[0] === undefined) {
        LATENCY_QUANTILES_LABELS.forEach(l => quantiles[l] = NaN);
        return quantiles;
    }
    Object.entries(LATENCY_QUANTILES).forEach(q => {
        const index = Math.floor(q[1] * sortedLatencies.length);
        quantiles[q[0]] = sortedLatencies[index];
    });
    return quantiles;
}

function getLatencyQuantilesPretty() {
    const quantiles = getLatencyQuantiles();
    return '|' + LATENCY_QUANTILES_LABELS
        .map(key => `${key} ${quantiles[key]}ms`)
        .join('|') + '|';
}

function getLatencyQuantilesCsv() {
    const quantiles = getLatencyQuantiles();
    return LATENCY_QUANTILES_LABELS.map(l => quantiles[l]).join(',');
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

function showStatus(stats) {
    const doneCount = stats.successCount + stats.errorCount;
    const completionRatio = doneCount / stats.totalCount;
    const completeCharCount = Math.floor(completionRatio * STATUS_BAR_LENGTH);
    const statusBarTplOffset = STATUS_BAR_LENGTH - completeCharCount;
    process.stdout.write(
        '\r['
            + STATUS_BAR_TPL.slice(statusBarTplOffset,
                                   statusBarTplOffset + STATUS_BAR_LENGTH)
            + `] `
            + `   ${Math.floor(doneCount / stats.totalCount * 100)}`.slice(-3)
            + `% ` + `        ${doneCount}`.slice(-8)
            + ` ops (${stats.errorCount} errors) `
            + `${`      ${isNaN(stats.opsPerSec) ? '' : stats.opsPerSec.toFixed(0)}`.slice(-6)} op/s `
            + `${`        ${isNaN(stats.kBPerSec) ? '' : stats.kBPerSec.toFixed(0)}`.slice(-8)} KB/s `
            + getLatencyQuantilesPretty() + '    ');
}

function outputCsvLine(stats) {
    fs.writeSync(
        csvStatsFile,
        `${Date.now()},${stats.opsPerSec},${stats.kBPerSec},${getLatencyQuantilesCsv()}\n`);
}

function ingest(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }
    console.log(`
    endpoint:            ${options.endpoint}
    prefix:              ${options.prefix}
    bucket:              ${options.bucket}
    workers:             ${options.workers}
    object count:        ${options.count}
    object size:         ${options.size}
    one object:          ${options.oneObject ? 'yes' : 'no'}
    del after put:       ${options.deleteAfterPut ? 'yes' : 'no'}
    rate limit:          ${options.rateLimit ? `${options.rateLimit} op/s` : 'none'}
    CSV output:          ${options.csvStats ? options.csvStats : 'none'}
    CSV output interval: ${options.csvStats ? `${options.csvStatsInterval} s` : 'N/A'}
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

    function getMashedStats() {
        const doneCount = successCount + errorCount;
        const doneCountInPeriod = doneCount - queryStatsWindow().doneCount;
        updateStatsWindow({ doneCount });
        const opsPerSec = (doneCountInPeriod * 1000) / STATS_PERIOD_MS;
        const kBPerSec = (doneCountInPeriod * options.size) / STATS_PERIOD_MS;

        return {
            totalCount: options.count,
            successCount,
            errorCount,
            opsPerSec,
            kBPerSec,
        };
    }

    function updateStatusBarIntervalFunc() {
        showStatus(getMashedStats());
    }
    const updateStatusBarInterval =
          setInterval(updateStatusBarIntervalFunc, STATUS_UPDATE_PERIOD_MS);


    function outputCsvLineIntervalFunc() {
        outputCsvLine(getMashedStats());
    }

    let csvStatsInterval;
    if (options.csvStats) {
        csvStatsFile = fs.openSync(options.csvStats, 'w');
        fs.writeSync(
            csvStatsFile,
            ['time',
             'opsPerSec',
             'kBPerSec']
                .concat(LATENCY_QUANTILES_LABELS.map(l => `q-ms:${l}`))
                .join(',') + '\n');
        csvStatsInterval =
            setInterval(outputCsvLineIntervalFunc,
                        options.csvStatsInterval * 1000);
    }
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
            const endSuccess = () => {
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
                    return next();
                }
                if (!options.deleteAfterPut) {
                    return endSuccess();
                }
                return s3.deleteObject({
                    Bucket: options.bucket,
                    Key: key,
                }, err => {
                    if (err) {
                        console.error(`error during "DELETE ${options.bucket}/${key}":`,
                                      err.message);
                        ++errorCount;
                        return next();
                    }
                    return endSuccess();
                });
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
        updateStatusBarIntervalFunc();
        console.log();
        clearInterval(updateStatusBarInterval);
        if (csvStatsFile) {
            fs.closeSync(csvStatsFile);
            clearInterval(csvStatsInterval);
        }
        cb();
    });
}

module.exports = ingest;
