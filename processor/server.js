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
Object.defineProperty(exports, "__esModule", { value: true });
const xpath = __importStar(require("xpath"));
const dom = __importStar(require("xmldom"));
const azure_storage_stream_1 = require("azure-storage-stream");
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
            context.log.verbose("validating configuration...");
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
        if (context.log)
            context.log.verbose("validated succesfully.");
        // establish connections
        const blob = new azure_storage_stream_1.AzureBlob({
            account: STORAGE_ACCOUNT,
            sas: STORAGE_SAS,
            key: STORAGE_KEY,
        });
        // get the message
        if (context.log)
            context.log.verbose(`getting the message from queue "processing"...`);
        const message = context.bindings.queue;
        if (context.log)
            context.log.verbose(`successfully got the message from queue "processing".`);
        // start loading filenames
        if (context.log)
            context.log.verbose(`getting blobs from "${STORAGE_CONTAINER_INPUT}/${message.partition}"...`);
        const fileLoader = blob.load(STORAGE_CONTAINER_INPUT, message.filenames);
        // start loading schemas
        if (context.log)
            context.log.verbose(`getting schemas from "${STORAGE_CONTAINER_SCHEMAS}"...`);
        const schemaLoader = blob.loadStream({
            transform: data => new azure_storage_stream_1.AzureBlobLoadOperation(STORAGE_CONTAINER_SCHEMAS, data.name)
        }, {
            transform: data => JSON.parse(data)
        });
        blob.list(STORAGE_CONTAINER_SCHEMAS).pipe(schemaLoader.in);
        // wait for all files and schemas to be loaded
        await Promise.all([
            fileLoader.waitForEnd(),
            schemaLoader.out.waitForEnd()
        ]);
        // batch up the rows so it can write more efficiently
        const batch = [];
        // apply schemas as files are loaded
        fileLoader.on("data", (data) => {
            const doc = new dom.DOMParser().parseFromString(data);
            for (const s of schemaLoader.out.buffer) {
                if (xpath.select(s.identify, doc).length > 0) {
                    if (context.log)
                        context.log.verbose(`schema identified as "${s.name}".`);
                    // extract the columns
                    const row = [];
                    for (const column of s.columns) {
                        const enclosure = column.enclosure || "";
                        const _default = column.default || "";
                        const value = xpath.select1(`string(${column.path})`, doc);
                        if (value) {
                            row.push(`${enclosure}${value}${enclosure}`);
                        }
                        else {
                            row.push(`${enclosure}${_default}${enclosure}`);
                        }
                    }
                    // buffer the row
                    const filename = `${message.partition}/${s.filename}`;
                    let entry = batch.find(f => f.filename === filename);
                    if (!entry) {
                        entry = new azure_storage_stream_1.AzureBlobCommitOperation("append", STORAGE_CONTAINER_OUTPUT, filename, "");
                        batch.push(entry);
                    }
                    entry.content += row.join(",") + "\n";
                }
            }
        });
        // wait for files to be fully processed
        await fileLoader.waitForEnd();
        // flush all the writes (with concurrency)
        await blob.commitAsync(batch);
    }
    catch (error) {
        // TODO: also log to some permanent space
        if (context.log)
            context.log.error(error.stack);
    }
    // respond
    context.done();
}
exports.run = run;
