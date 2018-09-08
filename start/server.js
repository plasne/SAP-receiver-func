"use strict";
// notes:
//  "azure-fucntions-ts-essentials" required "npm install -D types/node"
//  "es6-promise-pool" required "dom" being included in the library
//  host.json must use "Trace" to see "verbose" logs
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
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const azs = __importStar(require("azure-storage"));
const es6_promise_pool_1 = __importDefault(require("es6-promise-pool"));
const util = __importStar(require("util"));
// variables
const AZURE_WEB_JOBS_STORAGE = process.env.AzureWebJobsStorage;
const STORAGE_ACCOUNT = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_OUTPUT = process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS = process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS = process.env.STORAGE_SAS;
const STORAGE_KEY = process.env.STORAGE_KEY;
// get a segment of block blobs
function listSegment(service, container, all, token = null) {
    return new Promise((resolve, reject) => {
        service.listBlobsSegmented(container, token, (error, result) => {
            if (!error) {
                for (const entry of result.entries) {
                    if (entry.blobType === "BlockBlob")
                        all.push(entry);
                }
                if (result.continuationToken) {
                    listSegment(service, container, all, result.continuationToken).then(() => {
                        resolve();
                    }, error => {
                        reject(error);
                    });
                }
                else {
                    resolve();
                }
            }
            else {
                reject(error);
            }
        });
    });
}
// list all block blobs in container
async function list(context, service, container, pattern) {
    // get all blobs
    const all = [];
    await listSegment(service, container, all);
    if (context.log)
        context.log.verbose(`${all.length} Block Blobs found.`);
    // filter if a pattern should be applied
    if (pattern) {
        return all.filter(entry => {
            pattern.lastIndex = 0; // reset
            return pattern.test(entry.name);
        });
    }
    else {
        return all;
    }
}
/*
// read the contents of a block blob
function read(service: azs.BlobService, container: string, filename: string) {
    return new Promise<string>((resolve, reject) => {
        service.getBlobToText(container, filename, (error, result) => {
            if (!error) {
                resolve(result);
            } else {
                reject(error);
            }
        });
    });
}

// create an append blob with the first row
function createAppend(service: azs.BlobService, container: string, filename: string, content: string) {
    return new Promise((resolve, reject) => {
        service.createAppendBlobFromText(container, filename, content, (error) => {
            if (!error) {
                resolve();
            } else {
                reject(error);
            }
        });
    });
}

// create the container if it doesn't exist
function createContainer(service: azs.BlobService, container: string) {
    return new Promise((resolve, reject) => {
        service.createContainerIfNotExists(container, (error) => {
            if (!error) {
                resolve();
            } else {
                reject(error);
            }
        });
    });
}
*/
// load all schemas
async function getSchemas(context, service, container) {
    const schemas = [];
    // promisify
    const getBlobToText = util.promisify(azs.BlobService.prototype.getBlobToText).bind(service);
    // get a list of all schema files
    const blobs = await list(context, service, container, /.+\.json$/g);
    if (context.log)
        context.log.verbose(`${blobs.length} schemas found.`);
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
        }
        else {
            return undefined;
        }
    };
    // load them 10 at a time
    const pool = new es6_promise_pool_1.default(producer, 10);
    await pool.start();
    return schemas;
}
// module
async function run(context) {
    try {
        // validate
        if (!AZURE_WEB_JOBS_STORAGE)
            throw new Error("AzureWebJobsStorage is not defined.");
        if (!STORAGE_ACCOUNT)
            throw new Error("STORAGE_ACCOUNT is not defined.");
        if (!STORAGE_CONTAINER_OUTPUT)
            throw new Error("STORAGE_CONTAINER_OUTPUT is not defined.");
        if (!STORAGE_CONTAINER_SCHEMAS)
            throw new Error("STORAGE_CONTAINER_SCHEMAS is not defined.");
        if (!STORAGE_SAS && !STORAGE_KEY)
            throw new Error("STORAGE_SAS or STORAGE_KEY must be defined.");
        if (!context.req || !context.res)
            throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.originalUrl)
            throw new Error("The URL could not be determined.");
        // use the appropriate method to connect to the Blob service+
        const service = (() => {
            if (STORAGE_SAS)
                return azs.createBlobServiceWithSas(`https://${STORAGE_ACCOUNT}.blob.core.windows.net`, STORAGE_SAS);
            if (STORAGE_KEY)
                return azs.createBlobService(STORAGE_ACCOUNT, STORAGE_KEY);
            return undefined;
        })();
        if (!service)
            throw new Error("A connection could not be made to the Blob service.");
        // promisify
        const createContainerIfNotExists = util.promisify(azs.BlobService.prototype.createContainerIfNotExists).bind(service);
        const createOrReplaceAppendBlob = util.promisify(azs.BlobService.prototype.createOrReplaceAppendBlob).bind(service);
        const appendBlockFromText = util.promisify(azs.BlobService.prototype.appendBlockFromText).bind(service);
        // read schemas (with some parallelism)
        const schemas = await getSchemas(context, service, STORAGE_CONTAINER_SCHEMAS);
        // create the output container if it doesn't already exist
        await createContainerIfNotExists(STORAGE_CONTAINER_OUTPUT);
        // create file for each schema with header
        for (const schema of schemas) {
            if (context.log)
                context.log.verbose("schema: " + schema.name);
            const headers = [];
            for (const column of schema.columns) {
                headers.push(column.header);
            }
            const partitions = schema.partitions || 1;
            if (context.log)
                context.log.verbose("paritions: " + partitions);
            for (let partition = 0; partition < partitions; partition++) {
                const filename = schema.filename.replace("${partition}", partition);
                if (context.log)
                    context.log.verbose("filename: " + filename);
                await createOrReplaceAppendBlob(STORAGE_CONTAINER_OUTPUT, filename);
                const s = await appendBlockFromText(STORAGE_CONTAINER_OUTPUT, filename, headers.join(",") + "\n");
                if (context.log)
                    context.log.verbose(s);
                // change to append
                //write(service, container, filename, headers)
            }
        }
        // enqueue a list of files
        // find out if anything is in the queue
        // respond with status
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        context.res.body = schemas;
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
