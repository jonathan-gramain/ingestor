const async = require('async');
const http = require('http');
const fs = require('fs');
const AWS = require('aws-sdk');
const lineReader = require('line-reader');

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

const statsWindow = new Array(Math.max(
    Math.floor(STATS_PERIOD_MS / STATUS_UPDATE_PERIOD_MS),
    1)).fill({});
let statsWindowIndex = 0;

const latenciesWindow = [];
let latenciesWindowPos = 0;

let csvStatsFile;
let keysFromFileReader;

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

function create(options) {
    if (!options.endpoint) {
        return { options };
    }
    const credentials = new AWS.SharedIniFileCredentials({
        profile: options.profile,
    });
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
    return {
        options,
        s3,
    };
}

function showOptions(batchObj) {
    const { options } = batchObj;
    process.stdout.write(`
    endpoint:            ${options.endpoint}
    prefix:              ${options.prefix}
    bucket:              ${options.bucket}
    workers:             ${options.workers}
    object count:        ${options.count}
    object size:         ${options.size ? options.size : 'N/A'}
    rate limit:          ${options.rateLimit ? `${options.rateLimit} op/s` : 'none'}
    CSV output:          ${options.csvStats ? options.csvStats : 'none'}
    CSV output interval: ${options.csvStats ? `${options.csvStatsInterval} s` : 'N/A'}
    keys from file:      ${options.keysFromFile ? options.keysFromFile : 'none'}
`);
}

function getGeneratedKey(batchObj, n) {
    const { options } = batchObj;
    if (options.oneObject) {
        return `${options.prefix}test-key`;
    }
    let componentsOfN = [];
    let compWidth;
    if (options.limitPerDelimiter) {
        const delimiterCount = Math.ceil(
            Math.log(options.count) / Math.log(options.limitPerDelimiter) - 1);
        let _n = n;
        while (_n > 0) {
            componentsOfN.push(_n % options.limitPerDelimiter);
            _n = Math.floor(_n / options.limitPerDelimiter);
        }
        while (componentsOfN.length <= delimiterCount) {
            componentsOfN.push(0);
        }
        compWidth = Math.ceil(Math.log10(options.limitPerDelimiter));
    } else {
        componentsOfN.push(n);
        compWidth = Math.ceil(Math.log10(options.count));
    }
    componentsOfN.reverse();
    const compMask = Buffer.alloc(compWidth).fill('0').toString();
    const suffix = componentsOfN.map(comp => {
        return `${compMask}${comp}`.slice(-compWidth);
    }).join('/');
    return `${options.prefix}${suffix}`;
}

function openKeysFromFileReader(batchObj, cb) {
    const { options } = batchObj;
    lineReader.open(options.keysFromFile, (err, reader) => {
        if (err) {
            console.error('cannot open keys file:', err);
            return cb(err);
        }
        keysFromFileReader = reader;
        return cb();
    });
}

function init(batchObj, cb) {
    const { options } = batchObj;
    if (options.keysFromFile) {
        return openKeysFromFileReader(batchObj, cb);
    }
    return process.nextTick(cb);
}

function getKey(batchObj, n, cb) {
    const { options } = batchObj;

    let key;
    if (keysFromFileReader && keysFromFileReader.hasNextLine()) {
        return keysFromFileReader.nextLine((err, line) => {
            if (err) {
                console.error('error reading next key from file:', err);
                return cb(getGeneratedKey(batchObj, n));
            }
            return cb(line.trimRight());
        });
    }
    return cb(getGeneratedKey(batchObj, n));
}

function run(batchObj, batchOp, cb) {
    const { options, s3 } = batchObj;
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
            ['time', 'opsPerSec', 'kBPerSec']
                .concat(LATENCY_QUANTILES_LABELS.map(l => `q-ms:${l}`))
                .join(',') + '\n');
        csvStatsInterval =
            setInterval(outputCsvLineIntervalFunc,
                        options.csvStatsInterval * 1000);
    }
    let nextTime = options.rateLimit ? Date.now() : null;
    async.timesLimit(options.count, options.workers, (n, next) => {
        const startTime = Date.now();
        const doOp = () => {
            const opStartTime = Date.now();
            const endSuccess = () => {
                ++successCount;
                const endTime = Date.now();
                addLatency(endTime - opStartTime);
                next();
            };
            const endError = () => {
                ++errorCount;
                next();
            };
            batchOp(s3, n, endSuccess, endError);
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
        if (keysFromFileReader) {
            keysFromFileReader.close(cb);
        } else {
            cb();
        }
    });
}

module.exports = {
    showOptions,
    create,
    init,
    getKey,
    run,
};
