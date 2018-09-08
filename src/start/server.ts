
// notes:
//  "azure-fucntions-ts-essentials" required "npm install -D types/node"
//  "es6-promise-pool" required "dom" being included in the library
//  host.json must use "Trace" to see "verbose" logs

// includes
import { Context, HttpStatusCode } from "azure-functions-ts-essentials";
import * as azs from "azure-storage";
import PromisePool from "es6-promise-pool";
import * as util from "util";

// variables
const AZURE_WEB_JOBS_STORAGE:    string | undefined = process.env.AzureWebJobsStorage;
const STORAGE_ACCOUNT:           string | undefined = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_OUTPUT:  string | undefined = process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS: string | undefined = process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS:               string | undefined = process.env.STORAGE_SAS;
const STORAGE_KEY:               string | undefined = process.env.STORAGE_KEY;

// get a segment of block blobs
function listSegment(service: azs.BlobService, container: string, all: azs.BlobService.BlobResult[], token: azs.common.ContinuationToken | null = null) {


    
    return new Promise<void>((resolve, reject) => {
        service.listBlobsSegmented(container, token, (error, result) => {
            if (!error) {
                for (const entry of result.entries) {
                    if (entry.blobType === "BlockBlob") all.push(entry);
                }
                if (result.continuationToken) {
                    listSegment(service, container, all, result.continuationToken).then(() => {
                        resolve();
                    }, error => {
                        reject(error)
                    });
                } else {
                    resolve();
                }
            } else {
                reject(error);
            }
        });
    });
}

// list all block blobs in container
async function list(context: Context, service: azs.BlobService, container: string, pattern?: RegExp) {

    // get all blobs
    const all: azs.BlobService.BlobResult[] = [];
    await listSegment(service, container, all);
    if (context.log) context.log.verbose(`${all.length} Block Blobs found.`);

    // filter if a pattern should be applied
    if (pattern) {
        return all.filter(entry => {
            pattern.lastIndex = 0; // reset
            return pattern.test(entry.name);
        });
    } else {
        return all;
    }

}

// load all schemas
async function getSchemas(context: Context, service: azs.BlobService, container: string) {
    const schemas: any = [];

    // promisify
    const getBlobToText: (container: string, blob: string) => Promise<string> =
        util.promisify(azs.BlobService.prototype.getBlobToText).bind(service);

    // get a list of all schema files
    const blobs = await list(context, service, container, /.+\.json$/g);
    if (context.log) context.log.verbose(`${blobs.length} schemas found.`);

    // produce promises to load the files
    let index = 0;
    const producer = () => {
        if (index < blobs.length) {
            const blob = blobs[index];
            index++;
            return getBlobToText(container, blob.name).then(raw => {
                const obj = JSON.parse(raw);
                schemas.push(obj);
            });
        } else {
            return undefined;
        }
    }

    // load them 10 at a time
    const pool = new PromisePool(producer, 10);
    await pool.start();

    return schemas;
}

// module
export async function run(context: Context) {
    try {

        // validate
        if (!AZURE_WEB_JOBS_STORAGE) throw new Error("AzureWebJobsStorage is not defined.");
        if (!STORAGE_ACCOUNT) throw new Error("STORAGE_ACCOUNT is not defined.");
        if (!STORAGE_CONTAINER_OUTPUT) throw new Error("STORAGE_CONTAINER_OUTPUT is not defined.");
        if (!STORAGE_CONTAINER_SCHEMAS) throw new Error("STORAGE_CONTAINER_SCHEMAS is not defined.");
        if (!STORAGE_SAS && !STORAGE_KEY) throw new Error("STORAGE_SAS or STORAGE_KEY must be defined.");
        if (!context.req || !context.res) throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.originalUrl) throw new Error("The URL could not be determined.");

        // use the appropriate method to connect to the Blob service+
        const service = (() => {
            if (STORAGE_SAS) return azs.createBlobServiceWithSas(`https://${STORAGE_ACCOUNT}.blob.core.windows.net`, STORAGE_SAS);
            if (STORAGE_KEY) return azs.createBlobService(STORAGE_ACCOUNT, STORAGE_KEY);
            return undefined;
        })();
        if (!service) throw new Error("A connection could not be made to the Blob service.");

        // promisify
        const createContainerIfNotExists: (container: string) => Promise<azs.BlobService.ContainerResult> =
            util.promisify(azs.BlobService.prototype.createContainerIfNotExists).bind(service);
        const createOrReplaceAppendBlob: (container: string, blob: string) => Promise<void> =
            util.promisify(azs.BlobService.prototype.createOrReplaceAppendBlob).bind(service);
        const appendBlockFromText: (container: string, blob: string, content: string) => Promise<azs.BlobService.BlobResult> =
            util.promisify(azs.BlobService.prototype.appendBlockFromText).bind(service);

        // read schemas (with some parallelism)
        const schemas = await getSchemas(context, service, STORAGE_CONTAINER_SCHEMAS);

        // create the output container if it doesn't already exist
        await createContainerIfNotExists(STORAGE_CONTAINER_OUTPUT);

        // create file for each schema with header
        for (const schema of schemas) {
            const headers: string[] = [];
            for (const column of schema.columns) {
                headers.push(column.header);
            }
            const partitions: number = schema.partitions || 1;
            for (let partition = 0; partition < partitions; partition++) {
                const filename = schema.filename.replace("${partition}", partition);
                if (context.log) context.log.verbose(`schema "${schema.name}" is creating or replacing file "${STORAGE_CONTAINER_OUTPUT}/${filename}"...`);
                await createOrReplaceAppendBlob(STORAGE_CONTAINER_OUTPUT, filename);
                await appendBlockFromText(STORAGE_CONTAINER_OUTPUT, filename, headers.join(",") + "\n");
                if (context.log) context.log.verbose(`schema "${schema.name}" successfully wrote "${STORAGE_CONTAINER_OUTPUT}/${filename}".`);
            }
        }

        // enqueue a list of files
        

        // respond with status
        context.res.status = HttpStatusCode.OK;
        context.res.body = schemas;

    } catch (error) {
        if (context.res) context.res.status = HttpStatusCode.InternalServerError;
        if (context.log) context.log.error(error.stack);
    }

    // respond
    context.done();
    
}