const ingestor = require('commander');

const ingest = require('./ingest');
const ingest_mpu = require('./ingest_mpu');
const readall = require('./readall');
const ingest_level = require('./ingest_level');

ingestor.version('0.1');
ingestor.command('ingest')
    .option('--endpoint <endpoint>', 'endpoint URL')
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
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            isNaN(options.workers) ||
            isNaN(options.count) ||
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
            if (isNaN(options.count)) {
                console.error('value of option --count must be an integer');
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
    .option('--endpoint <endpoint>', 'endpoint URL')
    .option('--bucket <bucket>', 'bucket name')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--parts [nparts]', 'number of parts', 10, parseInt)
    .option('--size [n]', 'size of individual parts in bytes', 1000, parseInt)
    .option('--prefix [prefix]', 'key prefix', '')
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

ingestor.command('readall')
    .option('--endpoint <endpoint>', 'endpoint URL')
    .option('--bucket <bucket>', 'bucket name')
    .option('--prefix <prefix>', 'key prefix')
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
    .action(options => {
        if (!options.endpoint ||
            !options.bucket ||
            !options.prefix ||
            isNaN(options.workers) ||
            isNaN(options.count)) {
            if (!options.endpoint) {
                console.error('option --endpoint is missing');
            }
            if (!options.bucket) {
                console.error('option --bucket is missing');
            }
            if (!options.prefix) {
                console.error('option --prefix is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count)) {
                console.error('value of option --count must be an integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        readall(options, code => process.exit(code));
    });

ingestor.command('ingest_level')
    .option('--db-path <dbPath>', 'path to levelDB database')
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many keys total', 100, parseInt)
    .option('--size [n]', 'size of individual values in bytes', 1000, parseInt)
    .option('--batch-size [n]', 'number of keys per batch', 0, parseInt)
    .option('--sync', 'synchronous writes', false)
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
    .action(options => {
        if (!options.dbPath ||
            isNaN(options.workers) ||
            isNaN(options.count) ||
            isNaN(options.size)) {
            if (!options.dbPath) {
                console.error('option --db-path is missing');
            }
            if (isNaN(options.workers)) {
                console.error('value of option --workers must be an integer');
            }
            if (isNaN(options.count)) {
                console.error('value of option --count must be an integer');
            }
            if (isNaN(options.size)) {
                console.error('value of option --size must be an integer');
            }
            ingestor.outputHelp();
            process.exit(1);
        }
        ingest_level(options, code => process.exit(code));
    });

const commandName = process.argv[2];

if (!ingestor.commands.find(cmd => cmd._name === commandName)) {
    ingestor.outputHelp();
    process.exit(1);
}

ingestor.parse(process.argv);
