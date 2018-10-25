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
const es6_promise_pool_1 = __importDefault(require("es6-promise-pool"));
const azure_storage_stream_1 = require("azure-storage-stream");
// function to cast to a number or use a default if thats not possible
function valueOrDefault(value, _default) {
    if (value) {
        const i = parseInt(value);
        return (Number.isNaN(i)) ? _default : i;
    }
    else {
        return _default;
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
        if (!AZURE_WEB_JOBS_STORAGE)
            throw new Error("AzureWebJobsStorage is not defined.");
        if (!STORAGE_ACCOUNT)
            throw new Error("STORAGE_ACCOUNT is not defined.");
        if (!STORAGE_CONTAINER_INPUT)
            throw new Error("STORAGE_CONTAINER_INPUT is not defined.");
        if (!STORAGE_CONTAINER_OUTPUT)
            throw new Error("STORAGE_CONTAINER_OUTPUT is not defined.");
        if (!STORAGE_CONTAINER_SCHEMAS)
            throw new Error("STORAGE_CONTAINER_SCHEMAS is not defined.");
        if (!STORAGE_SAS && !STORAGE_KEY)
            throw new Error("STORAGE_SAS or STORAGE_KEY must be defined.");
        if (!context.req || !context.res)
            throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.query || !context.req.query.partition)
            throw new Error("The partition could not be determined.");
        if (context.log)
            context.log.verbose("validated succesfully");
        const partition = context.req.query.partition;
        // establish connections
        const input = new azure_storage_stream_1.AzureBlob({
            account: STORAGE_ACCOUNT,
            sas: STORAGE_SAS,
            key: STORAGE_KEY,
            container: STORAGE_CONTAINER_INPUT
        });
        const output = new azure_storage_stream_1.AzureBlob({
            service: input.service,
            container: STORAGE_CONTAINER_OUTPUT
        });
        const schema = new azure_storage_stream_1.AzureBlob({
            service: input.service,
            container: STORAGE_CONTAINER_SCHEMAS
        });
        const queue = new QueueHelper({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            name: "processing",
            encoder: "base64"
        });
        // create an output stream for writing the files
        const createContainer = output.createContainerIfNotExists();
        const out = output.writeStream({
            startAfter: createContainer
        });
        out.on("error", (error) => {
            throw error;
        });
        // start loading schemas
        const schemaLoader = schema.loadStream({
            transform: data => data.name
        }, {});
        schema.list().pipe(schemaLoader.in);
        // as schemas are loaded, create the output files for each
        schemaLoader.out.on("data", (data, metadata) => {
            const obj = JSON.parse(data);
            if (context.log)
                context.log.verbose(`schema "${metadata.filename}" loaded.`);
            // create file for each schema with header
            const headers = [];
            for (const column of obj.columns) {
                headers.push(column.header);
            }
            out.push(new AzureBlobStreamWriteOperation("append", `${partition}/${obj.filename}`, headers.join(",") + "\n"));
        });
        // create a file to hold errors
        out.push(new AzureBlobStreamWriteOperation("append", `${partition}/errors.txt`));
        // start fetching blobs
        let fetched = 0, queued = 0;
        let isFetching = true;
        let statusTimer = undefined;
        const buffer = [];
        input.list(partition + "/", data => {
            if (/.+\.xml$/g.test(data.name)) {
                return data.name;
            }
            else {
                return null;
            }
        }).on("readable", () => {
            // start a progress timer
            if (!statusTimer) {
                statusTimer = setInterval(() => {
                    if (context.log)
                        context.log.verbose(`${fetched} blobs enumerated, ${queued} blobs queued for processing, ${buffer.length} in the buffer...`);
                }, 1000);
            }
        });
        // create the queue if necessary
        if (context.log)
            context.log.verbose(`creating queue "processing" (if necessary)...`);
        await queue.createQueueIfNotExists();
        if (context.log)
            context.log.verbose(`created queue "processing".`);
        // produce promises to enqueue them
        const producer = () => {
            if (buffer.length > 0) {
                const set = buffer.splice(0, FILES_PER_MESSAGE);
                const message = {
                    partition: partition,
                    filenames: set.map(s => s.name)
                };
                const encoded = JSON.stringify(message);
                const promise = queue.enqueueMessage(encoded).then(() => {
                    queued += set.length;
                });
                return promise;
            }
            else if (isFetching) {
                return new Promise(resolve => setTimeout(resolve, 1000));
            }
            else {
                return undefined;
            }
        };
        // enqueue them 10 at a time
        const pool = new es6_promise_pool_1.default(producer, 10);
        await pool.start();
        // done
        if (statusTimer)
            clearInterval(statusTimer);
        if (context.log)
            context.log.verbose(`Completed with ${fetched} blobs enumerated, ${queued} blobs queued for processing.`);
        // respond with status
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        context.res.body = { status: "started" };
    }
    catch (error) {
        if (context.res)
            context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.InternalServerError;
        if (context.log)
            context.log.error(error.stack);
    }
    // respond
    const elapsed_in_min = ((new Date()).valueOf() - start.valueOf()) / 1000 / 60;
    if (context.log)
        context.log.verbose(`start() took ${elapsed_in_min.toFixed(2)} minutes to complete.`);
    context.done();
}
exports.run = run;
