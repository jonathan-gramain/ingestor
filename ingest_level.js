const batch = require('./batch');
const level = require('level');

function generateBody(size) {
    return new Array(Math.ceil(size / 10)).fill().map(
        e => Math.random().toString(36).slice(2))
        .join('')
        .slice(0, size);
}

function ingest_level(options, cb) {
    if (!options.prefix) {
        options.prefix = `test-${new Date().toISOString().replace(/[:.]/g, '-')}/`;
    }

    const obj = batch.create(options);
    batch.showOptions(obj);

    const lvl = new level(options.dbPath);

    const pregeneratedBodies = [];
    for (let i = 0, totalSize = 0;
         i < 1000000 && totalSize < 500000000;
         ++i, totalSize += options.size) {
        pregeneratedBodies.push(generateBody(options.size));
    }
    const ingestOp = (s3, n, endSuccess, endError) => {
        const key = batch.getKey(obj, n);
        const levelOpts = {};
        if (options.sync) {
            levelOpts.sync = true;
        }
        if (options.batchSize) {
            const levelBatch = [];
            for (let i = 0; i < options.batchSize; ++i) {
                const body = pregeneratedBodies[
                    Math.floor(Math.random() * pregeneratedBodies.length)];
                levelBatch.push({
                    type: 'put',
                    key: `${key}-${i}`,
                    value: body,
                });
            }
            lvl.batch(levelBatch, levelOpts, err => {
                if (err) {
                    console.error(`error during "BATCH ${key}":`,
                                  err.message);
                    return endError();
                }
                return endSuccess();
            });
        } else {
            const body = pregeneratedBodies[
                Math.floor(Math.random() * pregeneratedBodies.length)];
            lvl.put(key, body, levelOpts, err => {
                if (err) {
                    console.error(`error during "PUT ${key}":`,
                                  err.message);
                    return endError();
                }
                return endSuccess();
            });
        }
    };
    batch.run(obj, ingestOp, cb);
}

module.exports = ingest_level;