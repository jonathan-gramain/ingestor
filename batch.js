const async = require('async');
const assert = require('assert');
const crypto = require('crypto');
const http = require('http');
const https = require('https');
const fs = require('fs');
const AWS = require('aws-sdk');
const lineReader = require('line-reader');

const { lcgInit, lcgGen, lcgReset } = require('./lcg');

const STATUS_BAR_LENGTH = 60;
const STATUS_BAR_TPL =
      new Array(STATUS_BAR_LENGTH).fill('=').concat(
          new Array(STATUS_BAR_LENGTH).fill(' ')).join('');
const STATUS_UPDATE_PERIOD_MS = 200;
const STATS_PERIOD_MS = 2000;
const STATS_QUANTILES_WINDOW_SIZE = 10000;

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

let csvStatsFile = null;
let keysFromFileReader = null;
let keyList = null;

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

function permuteIndex(batchObj, n, options) {
    let idx;
    let lcgCache;
    let lcgState;
    const opSelector = Math.random();
    let opType;
    if (batchObj.writeDoneN === -1 || opSelector > batchObj.deleteThreshold) {
        idx = batchObj.writeN;
        ++batchObj.writeN;
        lcgState = batchObj.wLcgState;
        lcgCache = batchObj.wLcgCache;
        opType = 'put';
    } else if (opSelector < batchObj.readThreshold) {
        idx = batchObj.readN % (batchObj.writeDoneN + 1);
        ++batchObj.readN;
        lcgState = batchObj.rdLcgState;
        lcgCache = batchObj.rdLcgCache;
        opType = 'get';
    } else if (opSelector < batchObj.rewriteThreshold) {
        idx = batchObj.rewriteN % (batchObj.writeDoneN + 1);
        ++batchObj.rewriteN;
        lcgState = batchObj.rwLcgState;
        lcgCache = batchObj.rwLcgCache;
        opType = 'put';
    } else {
        idx = batchObj.deleteN % (batchObj.writeDoneN + 1);
        ++batchObj.deleteN;
        lcgState = batchObj.delLcgState;
        lcgCache = batchObj.delLcgCache;
        opType = 'del';
    }
    if (options.random) {
        // probabilistically decide to either extend the current access
        // to the next key, or jump to the next random key
        if (options.medianSequenceLength > 1 && batchObj.seqLcgState &&
            Math.random() > 1 / options.medianSequenceLength) {
            ++batchObj.seqIdx;
            return { idx: batchObj.seqLcgState.n + batchObj.seqIdx, opType };
        }
        // base the whole next sequence to generate on the same LCG state
        batchObj.seqLcgState = lcgState;
        batchObj.seqIdx = 0;

        // generate the next randomized index
        if (lcgState.iter > idx) {
            lcgReset(lcgState);
        }
        while (lcgState.iter < idx) {
            lcgCache[lcgState.iter] = lcgGen(lcgState);
        }
        if (lcgState.iter === idx) {
            return { idx: lcgGen(lcgState), opType };
        }
        const cached = lcgCache[idx];
        delete lcgCache[idx];
        return { idx: cached, opType };
    }
    return { idx, opType };
}

function create(options) {
    const credentials = new AWS.SharedIniFileCredentials({
        profile: options.profile,
    });
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
    return {
        options,
        s3s,
    };
}

function showOptions(batchObj) {
    const { options } = batchObj;
    process.stdout.write(`
    endpoint(s):         ${options.endpoint}
    prefix:              ${options.prefix}
    bucket:              ${options.bucket}
    workers:             ${options.workers}
    object count:        ${options.count}
    object size:         ${options.size ? options.size : 'N/A'}
    rate limit:          ${options.rateLimit ? `${options.rateLimit} op/s` : 'none'}
    CSV output:          ${options.csvStats ? options.csvStats : 'none'}
    CSV output interval: ${options.csvStats ? `${options.csvStatsInterval} s` : 'N/A'}
    hash keys:           ${options.hashKeys ? 'yes' : 'no'}
    keys from file:      ${options.keysFromFile ? options.keysFromFile : 'none'}
`);
}

function getOp(batchObj, n) {
    const { options } = batchObj;
    const { idx, opType } = permuteIndex(batchObj, n, options);
    if (options.oneObject) {
        return { opType, keyIdx: idx, objKey: `${options.prefix}test-key` };
    }
    let componentsOfN = [];
    let compWidth;
    if (options.limitPerDelimiter) {
        const delimiterCount = Math.ceil(
            Math.log(options.count) / Math.log(options.limitPerDelimiter) - 1);
        let _n = idx;
        while (_n > 0) {
            componentsOfN.push(_n % options.limitPerDelimiter);
            _n = Math.floor(_n / options.limitPerDelimiter);
        }
        while (componentsOfN.length <= delimiterCount) {
            componentsOfN.push(0);
        }
        compWidth = Math.ceil(Math.log10(options.limitPerDelimiter));
    } else {
        componentsOfN.push(idx);
        compWidth = Math.ceil(Math.log10(options.count));
    }
    componentsOfN.reverse();
    const compMask = Buffer.alloc(compWidth).fill('0').toString();
    let suffixComponents = componentsOfN.map(comp => {
        return `${compMask}${comp}`.slice(-compWidth);
    });
    if (options.hashKeys) {
        suffixComponents = suffixComponents.map(
            keyComponent => crypto.createHash('md5').update(keyComponent).digest().toString('hex')
        );
    }
    const suffix = suffixComponents.join('/');
    return { opType, keyIdx: idx, objKey: `${options.prefix}${suffix}` };
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

function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
        const randIndex = Math.trunc(Math.random() * (i + 1));
        const randIndexVal = array[randIndex];
        array[randIndex] = array[i];
        array[i] = randIndexVal;
    }
}



function init(batchObj, cb) {
    const { options } = batchObj;
    if (options.keysFromFile) {
        return openKeysFromFileReader(batchObj, err => {
            if (err) {
                return cb(err);
            }
            if (!options.random) {
                return cb();
            }
            keyList = [];
            return async.whilst(
                () => keysFromFileReader.hasNextLine(),
                next => keysFromFileReader.nextLine((err, line) => {
                    if (err) {
                        console.error('error reading next key from file:', err);
                        return next(err);
                    }
                    const key = line.trimRight();
                    keyList.push(key);
                    next();
                }),
                err => {
                    if (err) {
                        return cb(err);
                    }
                    shuffleArray(keyList);
                    keysFromFileReader = null;
                    return cb();
                }
            );
        });
    }
    if (options.random) {
        batchObj.randSeed = Math.floor(Math.random() * 1000000000);
        batchObj.wLcgState = lcgInit(Number.parseInt(options.count), batchObj.randSeed);
        batchObj.wLcgCache = {};
        batchObj.rdLcgState = lcgInit(Number.parseInt(options.count), batchObj.randSeed);
        batchObj.rdLcgCache = {};
        batchObj.rwLcgState = lcgInit(Number.parseInt(options.count), batchObj.randSeed);
        batchObj.rwLcgCache = {};
        batchObj.delLcgState = lcgInit(Number.parseInt(options.count), batchObj.randSeed);
        batchObj.delLcgCache = {};
        batchObj.seqLcgState = null;
        batchObj.seqIdx = 0;
    }
    batchObj.doneSet = new Set();
    batchObj.writeDoneN = -1;
    batchObj.writeN = 0;
    batchObj.readN = 0;
    batchObj.rewriteN = 0;
    batchObj.deleteN = 0;
    batchObj.readThreshold = options.readPercent / 100;
    batchObj.rewriteThreshold = batchObj.readThreshold + options.rewritePercent / 100;
    batchObj.deleteThreshold = batchObj.rewriteThreshold + options.deletePercent / 100;
    return process.nextTick(cb);
}

function getKey(batchObj, n, cb) {
    const { options } = batchObj;

    async.waterfall([
        next => {
            if (keysFromFileReader && !keysFromFileReader.hasNextLine()) {
                return openKeysFromFileReader(batchObj, next);
            }
            return next();
        },
        next => {
            if (keysFromFileReader) {
                return keysFromFileReader.nextLine((err, line) => {
                    if (err) {
                        console.error('error reading next key from file:', err);
                        return next(err);
                    }
                    const key = line.trimRight();
                    return next(null, key);
                });
            }
            if (keyList) {
                return next(null, keyList[n % keyList.length]);
            }
            const op = getOp(batchObj, n);
            return process.nextTick(() => next(null, op));
        },
    ], (err, op) => {
        if (err) {
            process.exit(1);
        }
        if (options.verbose) {
            console.log(`next op: ${JSON.stringify(op)}`);
        }
        return cb(op);
    });
}

function run(batchObj, batchOp, cb) {
    const { options, s3s } = batchObj;
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
    const triggerOp = (n, opCb) => {
        const startTime = Date.now();
        const doOp = () => {
            const opStartTime = Date.now();
            const endSuccess = keyIdx => {
                ++successCount;
                const endTime = Date.now();
                addLatency(endTime - opStartTime);
                opCb(keyIdx);
            };
            const endError = keyIdx => {
                ++errorCount;
                opCb(keyIdx);
            };
            getKey(batchObj, n, ({ opType, keyIdx, objKey }) => {
                batchOp(s3s[n % s3s.length], n, opType, objKey,
                        () => endSuccess(keyIdx),
                        () => endError(keyIdx));
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
    };

    let n = 0;
    let nInFlight = 0;

    const finalizeCb = () => {
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
    };

    const opCb = keyIdx => {
        if (keyIdx > batchObj.writeDoneN) {
            batchObj.doneSet.add(keyIdx);
            while (batchObj.doneSet.has(batchObj.writeDoneN + 1)) {
                ++batchObj.writeDoneN;
                batchObj.doneSet.delete(batchObj.writeDoneN);
            }
        }
        if (n < options.count) {
            triggerOp(n, opCb);
            ++n;
        } else {
            --nInFlight;
            if (nInFlight === 0) {
                finalizeCb();
            }
        }
    };
    while (n < Math.min(options.count, options.workers)) {
        triggerOp(n, opCb);
        ++n;
        ++nInFlight;
    }
}

module.exports = {
    showOptions,
    create,
    init,
    getKey,
    run,
};
