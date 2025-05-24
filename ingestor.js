const { program: ingestor } = require('commander');

const ingest = require('./ingest');
const ingest_mpu = require('./ingest_mpu');
const ingest_buckets = require('./ingest_buckets');
const ingest_bucketd = require('./ingest_bucketd');
const readall = require('./readall');
const deleteall = require('./deleteall');
const deleteversions = require('./deleteversions');

function parseIntOpt(value, dummyPrevious) {
    return parseInt(value, 10);
}

function parseFloatOpt(value, dummyPrevious) {
    return parseFloat(value, 10);
}


ingestor.version('0.1');
ingestor.command('ingest')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--count [n]', 'how many objects total', parseIntOpt, 100)
    .option('--size [n]', 'size of individual objects in bytes', parseIntOpt, 1000)
    .option('--prefix [prefix]', 'key prefix', '')
    .option('--prefix-exists', 'read/rewrite/delete existing keys from the specified prefix, created from a previous ingestor invocation with the same parameters', false)
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            parseIntOpt, 0)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', parseIntOpt, 0)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            parseIntOpt, 10)
    .option('--one-object', 'hammer on a single object', false)
    .option('--delete-after-put', 'send deletes after objects are put', false)
    .option('--add-tags', 'add a random number of tags', false)
    .option('--hash-keys', 'hash keys after the prefix with a MD5 sum to make them unordered', false)
    .option('--append-key-hash', 'append a key MD5 hash after each key component, to lengthen the keys without changing their relative order', false)
    .option('--keys-from-file [path]', 'read keys from file')
    .option('--mpu-parts [nbparts]', 'create MPU objects with this many parts',
            parseIntOpt, 0)
    .option('--mpu-fuzz-repeat-complete-prob [probability]',
            'repeat an extra time the complete-mpu requests with this probability ' +
            '(it can lead to more than one extra complete-mpu for the same request)',
            parseFloatOpt, 0)
    .option('--random', 'randomize keys when reading from a file', false)
    .option('--verbose', 'increase verbosity', false)
    .option('--object-lock', 'lock ingested objects for one year in GOVERNANCE mode (the bucket must have object-lock enabled)', false)
    .option('--rewrite-percent', 'probability percentage of rewrites over existing objects', 0)
    .option('--median-sequence-length <length>', 'with --random: introduce probabilistic sequentiality in accesses (read/write) where consecutive keys are accessed with the given median sequence length (in number of keys)',
            parseFloatOpt, 0)
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers) ||
            isNaN(options.count) || options.count <= 0 ||
            isNaN(options.size)) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count) || options.count <= 0) {
                console.error('value of option --count must be a strictly positive integer');
            }
            if (isNaN(options.size)) {
                console.error('value of option --size must be an integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        ingest(options, code => process.exit(code));
    });

ingestor.command('ingest_mpu')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--parts [nparts]', 'number of parts', parseIntOpt, 10)
    .option('--size [n]', 'size of individual parts in bytes', parseIntOpt, 1000)
    .option('--prefix [prefix]', 'key prefix', '')
    .option('--no-complete', 'do not complete the MPU', false)
    .option('--abort', 'abort the MPU instead of completing it', false)
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers) ||
            isNaN(options.parts) ||
            isNaN(options.size)) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.parts)) {
                console.error('value of option --parts must be an integer');
            }
            if (isNaN(options.size)) {
                console.error('value of option --size must be an integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        ingest_mpu(options, code => process.exit(code));
    });

ingestor.command('ingest_buckets')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--count [n]', 'how many objects total', parseIntOpt, 100)
    .option('--prefix [prefix]', 'bucket prefix', '')
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', parseIntOpt, 0)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            parseIntOpt, 10)
    .action(options => {
        if (!options.endpoint ||
            isNaN(options.workers) ||
            isNaN(options.count) || options.count <= 0) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count) || options.count <= 0) {
                console.error('value of option --count must be a strictly positive integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        ingest_buckets(options, code => process.exit(code));
    });

ingestor.command('ingest_bucketd')
    .option('--endpoint <endpoint...>', 'bucketd endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--count [n]', 'how many objects total', parseIntOpt, 100)
    .option('--prefix [prefix]', 'key prefix', '')
    .option('--prefix-exists', 'read/rewrite/delete existing keys from the specified prefix, created from a previous ingestor invocation with the same parameters', false)
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            parseIntOpt, 0)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', parseIntOpt, 0)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            parseIntOpt, 10)
    .option('--one-object', 'hammer on a single object', false)
    .option('--hash-keys', 'hash keys after the prefix with a MD5 sum to make them unordered', false)
    .option('--append-key-hash', 'append a key MD5 hash after each key component, to lengthen the keys without changing their relative order', false)
    .option('--keys-from-file [path]', 'read keys from file')
    .option('--random', 'randomize keys when reading from a file', false)
    .option('--verbose', 'increase verbosity', false)
    .option('--versioned', 'use versioned PUT', false)
    .option('--read-percent <rp>', 'probability percentage of reads over existing objects',
            parseIntOpt, 0)
    .option('--rewrite-percent <rwp>', 'probability percentage of rewrites over existing objects',
            parseIntOpt, 0)
    .option('--delete-percent <dp>', 'probability percentage of deletes over existing objects',
            parseIntOpt, 0)
    .option('--median-sequence-length <length>', 'with --random: introduce probabilistic sequentiality in accesses (read/write) where consecutive keys are accessed with the given median sequence length (in number of keys)',
            parseFloatOpt, 0)
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers) ||
            isNaN(options.count) || options.count <= 0 ||
            isNaN(options.count)) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count) || options.count <= 0) {
                console.error('value of option --count must be a strictly positive integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        const sumPercent = options.readPercent + options.rewritePercent + options.deletePercent;
        if (sumPercent >= 100) {
            console.error(`sum of --read-percent, --rewrite-percent and --delete-percent exceed or equal 100 (${sumPercent})`);
            process.exit(1);
        }
        ingest_bucketd(options, code => process.exit(code));
    });

ingestor.command('readall')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--prefix [prefix]', 'key prefix')
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            parseIntOpt, 0)
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--count [n]', 'how many objects total', parseIntOpt, 100)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', parseIntOpt, 0)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            parseIntOpt, 10)
    .option('--random',
            'randomize reads, while still reading all keys exactly once',
            false)
    .option('--keys-from-file [path]', 'read keys from file')
    .option('--median-sequence-length <length>', 'with --random: introduce probabilistic sequentiality in accesses (read/write) where consecutive keys are accessed with the given median sequence length (in number of keys)',
            parseFloatOpt, 0)
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers) ||
            isNaN(options.count) || options.count <= 0) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count) || options.count <= 0) {
                console.error('value of option --count must be a strictly positive integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        readall(options, code => process.exit(code));
    });

ingestor.command('deleteall')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--prefix [prefix]', 'key prefix')
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            parseIntOpt, 0)
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--count [n]', 'how many objects total', parseIntOpt, 100)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', parseIntOpt, 0)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            parseIntOpt, 10)
    .option('--random',
            'randomize deletes, while still deleting all keys exactly once',
            false)
    .option('--keys-from-file [path]', 'read keys from file')
    .option('--median-sequence-length <length>', 'with --random: introduce probabilistic sequentiality in accesses (read/write) where consecutive keys are accessed with the given median sequence length (in number of keys)',
            parseFloatOpt, 0)
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers) ||
            isNaN(options.count) || options.count <= 0) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count) || options.count <= 0) {
                console.error('value of option --count must be a strictly positive integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        deleteall(options, code => process.exit(code));
    });

ingestor.command('deleteversions')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--prefix [prefix]', 'key prefix')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', parseIntOpt, 10)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', parseIntOpt, 0)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            parseIntOpt, 10)
    .option('--random',
            'randomize deletes, while still deleting all keys exactly once',
            false)
    .option('--batch-size [count]',
            'size of individual batches in number of objects (default is no batching)')
    .option('--bypass-governance-retention', false)
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers)) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        deleteversions(options, code => process.exit(code));
    });

const commandName = process.argv[2];

if (!ingestor.commands.find(cmd => cmd._name === commandName)) {
    ingestor.outputHelp();
    process.exit(1);
}

ingestor.parse(process.argv);
