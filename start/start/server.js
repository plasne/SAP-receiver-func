"use strict";
// notes:
//  "azure-fucntions-ts-essentials" required "npm install -D types/node"
//  "es6-promise-pool" required "dom" being included in the library
//  host.json must use "Trace" to see "verbose" logs
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const BlobHelper_1 = __importDefault(require("../global/BlobHelper"));
const QueueHelper_1 = __importDefault(require("../global/QueueHelper"));
function valueOrDefault(value, _default) {
    if (value) {
        const i = parseInt(value);
        return (Number.isNaN(i)) ? _default : i;
    }
    else {
        return _default;
    }
}
function bridgeLogs(helper, context) {
    if (context.log) {
        helper.events.on("info", msg => { if (context.log)
            context.log.info(msg); });
        helper.events.on("verbose", msg => { if (context.log)
            context.log.verbose(msg); });
        helper.events.on("error", msg => { if (context.log)
            context.log.error(msg); });
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
        const input = new BlobHelper_1.default({
            account: STORAGE_ACCOUNT,
            sas: STORAGE_SAS,
            key: STORAGE_KEY,
            container: STORAGE_CONTAINER_INPUT
        });
        bridgeLogs(input, context);
        const output = new BlobHelper_1.default({
            service: input.service,
            container: STORAGE_CONTAINER_OUTPUT
        });
        bridgeLogs(output, context);
        const schema = new BlobHelper_1.default({
            service: input.service,
            container: STORAGE_CONTAINER_SCHEMAS
        });
        bridgeLogs(schema, context);
        const queue = new QueueHelper_1.default({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            name: "processing",
            encoder: "base64"
        });
        bridgeLogs(queue, context);
        // read schemas (with some parallelism)
        if (context.log)
            context.log.verbose(`getting schemas from "${STORAGE_CONTAINER_SCHEMAS}"...`);
        const schemaBlobs = await schema.list(/.+\.json$/g);
        const schemaFilenames = schemaBlobs.map(s => s.name);
        const schemas = await schema.loadFiles(schemaFilenames, "json");
        // create the output container if it doesn't already exist
        if (context.log)
            context.log.verbose(`creating output container "${STORAGE_CONTAINER_OUTPUT}" (if necessary)...`);
        output.createContainerIfNotExists();
        if (context.log)
            context.log.verbose(`created output container "${STORAGE_CONTAINER_OUTPUT}".`);
        // create file for each schema with header
        for (const s of schemas) {
            const headers = [];
            for (const column of s.columns) {
                headers.push(column.header);
            }
            const filename = `${partition}/${s.filename}`;
            if (context.log)
                context.log.verbose(`schema "${s.name}" is creating or replacing file "${STORAGE_CONTAINER_OUTPUT}/${filename}"...`);
            await output.createOrReplaceAppendBlob(filename);
            await output.appendToBlob(filename, headers.join(",") + "\n");
            if (context.log)
                context.log.verbose(`schema "${s.name}" successfully wrote "${STORAGE_CONTAINER_OUTPUT}/${filename}".`);
        }
        // create the queue if necessary
        if (context.log)
            context.log.verbose(`creating queue "processing" (if necessary)...`);
        await queue.createQueueIfNotExists();
        if (context.log)
            context.log.verbose(`created queue "processing".`);
        // get a complete list of blobs
        const blobs = await input.listWithPrefix(partition + "/", /.+\.xml$/g);
        // craft messages packing them per FILES_PER_MESSAGE
        let count = 0;
        const messages = [];
        for (let outer = 0; outer < blobs.length; outer += FILES_PER_MESSAGE) {
            const message = {
                partition: partition,
                filenames: []
            };
            const max = Math.min(outer + FILES_PER_MESSAGE, blobs.length);
            for (let inner = outer; inner < max; inner++) {
                message.filenames.push(blobs[inner].name);
                count++;
            }
            const encoded = JSON.stringify(message);
            messages.push(encoded);
        }
        // enqueue a list of files (with some parallelism)
        if (context.log)
            context.log.verbose(`started enqueuing filenames to "processing"...`);
        await queue.enqueueMessages(messages);
        if (context.log)
            context.log.verbose(`enqueued ${count} filenames to "processing".`);
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
    context.done();
}
exports.run = run;
