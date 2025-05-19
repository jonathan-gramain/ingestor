const { program: ingestor } = require('commander');

const ingest = require('./ingest');
const ingest_mpu = require('./ingest_mpu');
const ingest_buckets = require('./ingest_buckets');
const ingest_bucketd = require('./ingest_bucketd');
const readall = require('./readall');
const deleteall = require('./deleteall');
const deleteversions = require('./deleteversions');

ingestor.version('0.1');
ingestor.command('ingest')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many objects total', 100, parseInt)
    .option('--size [n]', 'size of individual objects in bytes', 1000, parseInt)
    .option('--prefix [prefix]', 'key prefix', '')
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            0, parseInt)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', 0, parseInt)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            10, parseInt)
    .option('--one-object', 'hammer on a single object', false)
    .option('--delete-after-put', 'send deletes after objects are put', false)
    .option('--add-tags', 'add a random number of tags', false)
    .option('--hash-keys', 'hash keys after the prefix with a MD5 sum to make them unordered', false)
    .option('--keys-from-file [path]', 'read keys from file')
    .option('--mpu-parts [nbparts]', 'create MPU objects with this many parts',
            0, parseInt)
    .option('--mpu-fuzz-repeat-complete-prob [probability]',
            'repeat an extra time the complete-mpu requests with this probability ' +
            '(it can lead to more than one extra complete-mpu for the same request)',
            0, parseFloat)
    .option('--random', 'randomize keys when reading from a file', false)
    .option('--verbose', 'increase verbosity', false)
    .option('--object-lock', 'lock ingested objects for one year in GOVERNANCE mode (the bucket must have object-lock enabled)', false)
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
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--parts [nparts]', 'number of parts', 10, parseInt)
    .option('--size [n]', 'size of individual parts in bytes', 1000, parseInt)
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
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many objects total', 100, parseInt)
    .option('--prefix [prefix]', 'bucket prefix', '')
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', 0, parseInt)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            10, parseInt)
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
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many objects total', 100, parseInt)
    .option('--prefix [prefix]', 'key prefix', '')
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            0, parseInt)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', 0, parseInt)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            10, parseInt)
    .option('--one-object', 'hammer on a single object', false)
    .option('--delete-after-put', 'send deletes after objects are put', false)
    .option('--hash-keys', 'hash keys after the prefix with a MD5 sum to make them unordered', false)
    .option('--keys-from-file [path]', 'read keys from file')
    .option('--random', 'randomize keys when reading from a file', false)
    .option('--verbose', 'increase verbosity', false)
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
        ingest_bucketd(options, code => process.exit(code));
    });

ingestor.command('readall')
    .option('--endpoint <endpoint...>', 'endpoint URL(s)')
    .option('--bucket <bucket>', 'bucket name')
    .option('--prefix [prefix]', 'key prefix')
    .option('--limit-per-delimiter [limit]',
            'max number of object to group in a single delimiter range',
            0, parseInt)
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many objects total', 100, parseInt)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', 0, parseInt)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            10, parseInt)
    .option('--random',
            'randomize reads, while still reading all keys exactly once',
            false)
    .option('--keys-from-file [path]', 'read keys from file')
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
            0, parseInt)
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many objects total', 100, parseInt)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', 0, parseInt)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            10, parseInt)
    .option('--random',
            'randomize deletes, while still deleting all keys exactly once',
            false)
    .option('--keys-from-file [path]', 'read keys from file')
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
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--rate-limit [n]',
            'limit rate of operations (in op/s)', 0, parseInt)
    .option('--csv-stats [filename]', 'output file for stats in CSV format')
    .option('--csv-stats-interval [n]',
            'interval in seconds between each CSV stats output line',
            10, parseInt)
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
