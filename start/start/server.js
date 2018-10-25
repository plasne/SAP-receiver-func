"use strict";
// FUNCTION: start()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// INPUT:    ?partition=<name_of_the_partition_to_process>
// PURPOSE:  This function:
//           1. creates a CSV file with headers but no rows for each schema definition
//           2. creates a CSV file to hold errors
//           3. reads all blobs from the specified partition
//           4. breaks the list of blobs into chunks of size FILES_PER_MESSAGE
//           5. enqueues the messages for the processor()
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// notes:
//  "azure-fucntions-ts-essentials" required "npm install -D types/node"
//  "es6-promise-pool" required "dom" being included in the library
//  host.json must use "Trace" to see "verbose" logs
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const AzureBlob_1 = __importDefault(require("../global/AzureBlob"));
const AzureBlobOperation_1 = __importDefault(require("../global/AzureBlobOperation"));
const AzureQueue_1 = __importDefault(require("../global/AzureQueue"));
const AzureQueueOperation_1 = __importDefault(require("../global/AzureQueueOperation"));
// function to cast to a number or use a default if thats not possible
function valueOrDefault(value, dflt) {
    if (value) {
        const i = parseInt(value, 10);
        return Number.isNaN(i) ? dflt : i;
    }
    else {
        return dflt;
    }
}
// variables
const AZURE_WEB_JOBS_STORAGE = process.env.AzureWebJobsStorage;
const STORAGE_ACCOUNT = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT = process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_CONTAINER_OUTPUT = process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS = process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS = process.env.STORAGE_SAS;
const STORAGE_KEY = process.env.STORAGE_KEY;
const FILES_PER_MESSAGE = valueOrDefault(process.env.FILES_PER_MESSAGE, 10);
// module
async function run(context) {
    const start = new Date();
    try {
        // validate
        if (!AZURE_WEB_JOBS_STORAGE) {
            throw new Error('AzureWebJobsStorage is not defined.');
        }
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
        if (!context.req || !context.res) {
            throw new Error('Request/Response must be defined in bindings.');
        }
        if (!context.req.query || !context.req.query.partition) {
            throw new Error('The partition could not be determined.');
        }
        if (context.log)
            context.log.verbose('validated succesfully');
        const partition = context.req.query.partition;
        // count errors
        let errors = 0;
        // establish connections
        const blob = new AzureBlob_1.default({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS
        });
        const queue = new AzureQueue_1.default({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            encoder: 'base64'
        });
        // create an output stream for writing the files
        const output = blob.streams({}, {
            processAfter: blob.createContainerIfNotExists(STORAGE_CONTAINER_OUTPUT)
        });
        output.out.on('error', (error) => {
            if (context.log)
                context.log.error(error);
            errors++;
        });
        // create a file to hold errors
        output.in.push(new AzureBlobOperation_1.default(STORAGE_CONTAINER_OUTPUT, 'createAppend', `${partition}/errors.txt`));
        // as schemas are loaded, create the output files for each
        blob.loadAsStream(STORAGE_CONTAINER_SCHEMAS)
            .on('data', (data, metadata) => {
            const obj = JSON.parse(data);
            if (context.log) {
                context.log.info(`schema "${metadata.filename}" loaded.`);
            }
            // create file for each schema with header
            const headers = [];
            for (const column of obj.columns) {
                headers.push(column.header);
            }
            output.in.push(new AzureBlobOperation_1.default(STORAGE_CONTAINER_OUTPUT, 'createAppend', `${partition}/${obj.filename}`, headers.join(',') + '\n'));
        })
            .on('end', () => {
            output.in.end();
        })
            .on('error', error => {
            if (context.log)
                context.log.error(error);
            errors++;
        });
        // start fetching blobs
        let queued = 0;
        const filenames = blob
            .listAsStream(STORAGE_CONTAINER_INPUT, partition, {
            transform: data => {
                if (/.+\.xml$/g.test(data.name)) {
                    return data.name;
                }
                else {
                    return null;
                }
            }
        })
            .on('error', error => {
            if (context.log)
                context.log.error(error);
            errors++;
        });
        // create an output stream for queue messages
        const enqueuer = queue.streams({}, {
            processAfter: queue.createQueueIfNotExists('processing')
        });
        enqueuer.out.on('error', (error) => {
            if (context.log)
                context.log.error(error);
            errors++;
        });
        // process the filenames every second
        const statusTimer = setInterval(() => {
            // batch
            if (filenames.buffer.length > 0) {
                const batch = filenames.buffer.splice(0, FILES_PER_MESSAGE);
                const message = {
                    filenames: batch,
                    partition
                };
                queued += batch.length;
                enqueuer.in.push(new AzureQueueOperation_1.default('processing', 'enqueue', message));
            }
            else if (filenames.state === 'ended') {
                enqueuer.in.end();
            }
            // log progress
            if (context.log) {
                context.log.info(`${queued} blobs queued for processing, ${filenames.buffer.length} in the buffer...`);
            }
        }, 1000);
        // wait for all operations to finish
        await Promise.all([output.out.waitForEnd(), enqueuer.out.waitForEnd()]);
        // done
        if (statusTimer)
            clearInterval(statusTimer);
        if (context.log) {
            context.log.info(`completed with ${queued} blobs queued for processing.`);
        }
        // respond with status
        if (errors > 0)
            throw new Error('errors were found in the streams');
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        context.res.body = { status: 'started' };
    }
    catch (error) {
        if (context.res) {
            context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.InternalServerError;
        }
        if (context.log)
            context.log.error(error.stack);
    }
    // respond
    const elapsedInMin = (new Date().valueOf() - start.valueOf()) / 1000 / 60;
    if (context.log) {
        context.log.verbose(`start() took ${elapsedInMin.toFixed(2)} minutes to complete.`);
    }
}
exports.run = run;
