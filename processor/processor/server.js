"use strict";
// FUNCTION: processor()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function:
//           1. loads all schemas
//           2. loads the set of files it was assigned from blob storage
//           3. attempts to apply each schema to each file generating rows
//           4. all rows updates are flushed to storage
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dom = __importStar(require("xmldom"));
const xpath = __importStar(require("xpath"));
const AzureBlob_1 = __importDefault(require("../global/AzureBlob"));
const AzureBlobOperation_1 = __importDefault(require("../global/AzureBlobOperation"));
// variables
const STORAGE_ACCOUNT = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT = process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_CONTAINER_OUTPUT = process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS = process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS = process.env.STORAGE_SAS;
const STORAGE_KEY = process.env.STORAGE_KEY;
// module
async function run(context) {
    try {
        // validate
        if (context.log)
            context.log.verbose('validating configuration...');
        if (!STORAGE_ACCOUNT) {
            throw new Error('STORAGE_ACCOUNT is not defined.');
        }
        if (!STORAGE_CONTAINER_INPUT) {
            throw new Error('STORAGE_CONTAINER_INPUT is not defined.');
        }
        if (!STORAGE_CONTAINER_OUTPUT) {
            throw new Error('STORAGE_CONTAINER_OUTPUT is not defined.');
        }
        if (!STORAGE_CONTAINER_SCHEMAS) {
            throw new Error('STORAGE_CONTAINER_SCHEMAS is not defined.');
        }
        if (!STORAGE_SAS && !STORAGE_KEY) {
            throw new Error('STORAGE_SAS or STORAGE_KEY must be defined.');
        }
        if (context.log)
            context.log.verbose('validated succesfully.');
        // get the message
        if (context.log) {
            context.log.info(`getting the message from queue "processing"...`);
        }
        const message = context.bindings.queue;
        if (context.log) {
            context.log.info(`successfully got the message from queue "processing" (${message.filenames.length} files).`);
        }
        // establish connections
        const blob = new AzureBlob_1.default({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS
        });
        // batch up the rows so it can write more efficiently
        const batch = [];
        // define error function
        const raise = (msg, partition, filename) => {
            const formatted = `${new Date().toISOString()} ${partition ? '[' + partition + ']' : ''} ${filename ? '[' + filename + ']' : ''} ${msg}\n`;
            batch.push(new AzureBlobOperation_1.default(STORAGE_CONTAINER_OUTPUT, 'append', `${message.partition}/errors.txt`, formatted));
        };
        // start processing
        try {
            // load all schemas
            if (context.log) {
                context.log.info(`getting schemas from "${STORAGE_CONTAINER_SCHEMAS}"...`);
            }
            const schemas = blob
                .loadAsStream(STORAGE_CONTAINER_SCHEMAS, undefined, {
                transform: data => JSON.parse(data)
            })
                .on('error', error => {
                if (context.log)
                    context.log.error(error);
            });
            await schemas.waitForEnd();
            // start processing files
            if (context.log) {
                context.log.info(`getting blobs from "${STORAGE_CONTAINER_INPUT}/${message.partition}"...`);
            }
            const loader = blob
                .stream(message.filenames, {
                transform: (filename) => new AzureBlobOperation_1.default(STORAGE_CONTAINER_INPUT, 'load', filename)
            })
                .on('data', (data, op) => {
                // start logging
                let schemasFound = 0;
                if (context.log) {
                    context.log.info(`looking for schemas for ${op.filename}...`);
                }
                // parse as XML
                let doc;
                try {
                    doc = new dom.DOMParser().parseFromString(data);
                }
                catch (error) {
                    if (context.log) {
                        context.log.info('the file was not properly formatted XML, it will be skipped.');
                    }
                    raise('the file was not properly formatted XML, it will be skipped.', message.partition, op.filename);
                }
                // see which schemas match
                if (doc) {
                    for (const s of schemas.buffer) {
                        if (xpath.select(s.identify, doc).length > 0) {
                            // identify as a found schema
                            if (context.log) {
                                context.log.info(`schema identified as "${s.name}".`);
                            }
                            schemasFound++;
                            // extract the columns
                            const row = [];
                            for (const column of s.columns) {
                                const enclosure = column.enclosure || '';
                                const dflt = column.default || '';
                                const value = xpath.select1(`string(${column.path})`, doc);
                                if (value) {
                                    row.push(`${enclosure}${value}${enclosure}`);
                                }
                                else {
                                    row.push(`${enclosure}${dflt}${enclosure}`);
                                }
                            }
                            // buffer the row
                            const filename = `${message.partition}/${s.filename}`;
                            let entry = batch.find(f => f.filename === filename);
                            if (!entry) {
                                entry = new AzureBlobOperation_1.default(STORAGE_CONTAINER_OUTPUT, 'append', filename, '');
                                batch.push(entry);
                            }
                            entry.content += row.join(',') + '\n';
                        }
                    }
                    // raise error if no schemas were found
                    if (schemasFound < 1) {
                        if (context.log) {
                            context.log.error('no schemas were found that match this file.');
                        }
                        raise('no schemas were found that match this file.', message.partition, op.filename);
                    }
                }
            })
                .on('error', (error, op) => {
                if (context.log)
                    context.log.error(error);
                raise(error.message, message.partition, op.filename);
            });
            // wait for files to be fully processed
            await loader.waitForEnd();
        }
        catch (error) {
            if (context.log)
                context.log.error(error.stack);
            raise(error.stack);
        }
        // flush all the writes
        if (context.log) {
            context.log.info(`flushing ${batch.length} write(s)...`);
        }
        const writer = blob.stream(batch);
        await writer.waitForEnd();
        if (context.log) {
            context.log.info(`all writes flushed.`);
        }
    }
    catch (error) {
        if (context.log)
            context.log.error(error.stack);
        process.exit(1);
    }
}
exports.run = run;
