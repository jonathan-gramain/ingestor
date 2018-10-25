const ingestor = require('commander');

const ingest = require('./ingest');

ingestor.version('0.1');
ingestor.command('ingest')
    .option('--endpoint <endpoint>', 'endpoint URL')
    .option('--bucket <bucket>', 'bucket name')
    .option('--profile [profile]', 'aws/credentials profile', 'default')
    .option('--workers [n]', 'how many parallel workers', 10, parseInt)
    .option('--count [n]', 'how many objects total', 100, parseInt)
    .option('--size [n]', 'size of individual objects in bytes', 1000, parseInt)
    .option('--prefix [prefix]', 'key prefix', '')
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

const commandName = process.argv[2];

if (!ingestor.commands.find(cmd => cmd._name === commandName)) {
    ingestor.outputHelp();
    process.exit(1);
}

ingestor.parse(process.argv);
