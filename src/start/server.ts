
// notes:
//  "azure-fucntions-ts-essentials" required "npm install -D types/node"
//  "es6-promise-pool" required "dom" being included in the library
//  host.json must use "Trace" to see "verbose" logs

// includes
import { Context, HttpStatusCode } from "azure-functions-ts-essentials";
import * as azs from "azure-storage";
import PromisePool from "es6-promise-pool";
import * as util from "util";
import { message } from "../global/custom";
import BlobHelper from "../global/BlobHelper";

function valueOrDefault(value: string | undefined, _default: number) {
    if (value) {
        const i = parseInt(value);
        return (Number.isNaN(i)) ? _default : i;
    } else {
        return _default;
    }
}

// variables
const AZURE_WEB_JOBS_STORAGE:    string | undefined = process.env.AzureWebJobsStorage;
const STORAGE_ACCOUNT:           string | undefined = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT:   string | undefined = process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_CONTAINER_OUTPUT:  string | undefined = process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS: string | undefined = process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS:               string | undefined = process.env.STORAGE_SAS;
const STORAGE_KEY:               string | undefined = process.env.STORAGE_KEY;
const FILES_PER_MESSAGE:         number = valueOrDefault(process.env.FILES_PER_MESSAGE, 10);

// module
export async function run(context: Context) {
    try {

        // validate
        if (!AZURE_WEB_JOBS_STORAGE) throw new Error("AzureWebJobsStorage is not defined.");
        if (!STORAGE_ACCOUNT) throw new Error("STORAGE_ACCOUNT is not defined.");
        if (!STORAGE_CONTAINER_INPUT) throw new Error("STORAGE_CONTAINER_INPUT is not defined.");
        if (!STORAGE_CONTAINER_OUTPUT) throw new Error("STORAGE_CONTAINER_OUTPUT is not defined.");
        if (!STORAGE_CONTAINER_SCHEMAS) throw new Error("STORAGE_CONTAINER_SCHEMAS is not defined.");
        if (!STORAGE_SAS && !STORAGE_KEY) throw new Error("STORAGE_SAS or STORAGE_KEY must be defined.");
        if (!context.req || !context.res) throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.query || !context.req.query.partition) throw new Error("The partition could not be determined.");
        if (context.log) context.log.verbose("validated succesfully");
        const partition = context.req.query.partition;

        // use the appropriate method to connect to the Blob service
        const blobService = (() => {
            if (STORAGE_SAS) return azs.createBlobServiceWithSas(`https://${STORAGE_ACCOUNT}.blob.core.windows.net`, STORAGE_SAS);
            if (STORAGE_KEY) return azs.createBlobService(STORAGE_ACCOUNT, STORAGE_KEY);
            return undefined;
        })();
        if (!blobService) throw new Error("A connection could not be made to the Blob service.");

        // read schemas (with some parallelism)
        if (context.log) context.log.verbose(`getting schemas from "${STORAGE_CONTAINER_SCHEMAS}"...`);
        const schemaBlobHelper = new BlobHelper(context, blobService, STORAGE_CONTAINER_SCHEMAS);
        const schemaBlobs = await schemaBlobHelper.list(/.+\.json$/g);
        const schemaFilenames = schemaBlobs.map(s => s.name);
        const schemas = await schemaBlobHelper.loadFiles(schemaFilenames);

        // create the output container if it doesn't already exist
        const outputBlobHelper = new BlobHelper(context, blobService, STORAGE_CONTAINER_OUTPUT);
        if (context.log) context.log.verbose(`creating output container "${STORAGE_CONTAINER_OUTPUT}" (if necessary)...`);
        outputBlobHelper.createContainerIfNotExists();
        if (context.log) context.log.verbose(`created output container "${STORAGE_CONTAINER_OUTPUT}".`);

        // create file for each schema with header
        for (const schema of schemas) {
            const headers: string[] = [];
            for (const column of schema.columns) {
                headers.push(column.header);
            }
            const filename = `${partition}/${schema.filename}`;
            if (context.log) context.log.verbose(`schema "${schema.name}" is creating or replacing file "${STORAGE_CONTAINER_OUTPUT}/${filename}"...`);
            await outputBlobHelper.createOrReplaceAppendBlob(filename);
            await outputBlobHelper.appendToBlob(filename, headers.join(",") + "\n");
            if (context.log) context.log.verbose(`schema "${schema.name}" successfully wrote "${STORAGE_CONTAINER_OUTPUT}/${filename}".`);
        }

        // connect to the Queue service
        const queueService = azs.createQueueService(AZURE_WEB_JOBS_STORAGE);

        // promisify
        const createQueueIfNotExists: (queue: string) => Promise<azs.QueueService.QueueResult> =
            util.promisify(azs.QueueService.prototype.createQueueIfNotExists).bind(queueService);
        const createMessage: (queue: string, message: string) => Promise<azs.QueueService.QueueMessageResult> =
            util.promisify(azs.QueueService.prototype.createMessage).bind(queueService);

        // enqueue a list of files (10 write operations at a time)
        if (context.log) context.log.verbose(`started enqueuing filenames to "processing"...`);
        let count = 0;

        const helper = new BlobHelper(context, blobService, STORAGE_CONTAINER_INPUT);

        if (context.log) context.log.verbose(`creating queue "processing" (if necessary)...`);
        await createQueueIfNotExists("processing");
        if (context.log) context.log.verbose(`created queue "processing".`);
        const blobs = await helper.listWithPrefix(partition + "/", /.+\.xml$/g);
        const pool = new PromisePool(() => {
            if (blobs.length < 1) return undefined;
            const group = blobs.splice(0, FILES_PER_MESSAGE);
            const message: message = {
                partition: partition,
                filenames: []
            };
            for (const blob of group) {
                message.filenames.push(blob.name);
            }
            count += group.length;
            if (count % (FILES_PER_MESSAGE * 100) === 0 && context.log) context.log.verbose(`enqueued ${count} filenames to "processing"...`);
            return createMessage("processing", JSON.stringify(message));
        }, 10);
        await pool.start();
        if (context.log) context.log.verbose(`enqueued ${count} filenames to "processing".`);

        // respond with status
        context.res.status = HttpStatusCode.OK;
        context.res.body = { status: "started" };

    } catch (error) {
        if (context.res) context.res.status = HttpStatusCode.InternalServerError;
        if (context.log) context.log.error(error.stack);
    }

    // respond
    context.done();
    
}