
// includes
import { Context } from "azure-functions-ts-essentials";
import { message } from "../global/custom";
import BlobHelper, { BlobHelperBatchWrite } from "../global/BlobHelper";
import * as xpath from "xpath";
import * as dom from "xmldom";

//import * as azs from "azure-storage";

function bridgeLogs(helper: BlobHelper, context: Context) {
    if (context.log) {
        helper.events.on("info", msg => { if (context.log) context.log.info(msg) });
        helper.events.on("verbose", msg => { if (context.log) context.log.verbose(msg) });
        helper.events.on("error", msg => { if (context.log) context.log.error(msg) });
    }
}

// variables
const STORAGE_ACCOUNT:           string | undefined = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT:   string | undefined = process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_CONTAINER_OUTPUT:  string | undefined = process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS: string | undefined = process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS:               string | undefined = process.env.STORAGE_SAS;
const STORAGE_KEY:               string | undefined = process.env.STORAGE_KEY;

// module
export async function run(context: Context) {
    try {

        // validate
        if (context.log) context.log.verbose("validating configuration...");
        if (!STORAGE_ACCOUNT) throw new Error("STORAGE_ACCOUNT is not defined.");
        if (!STORAGE_CONTAINER_INPUT) throw new Error("STORAGE_CONTAINER_INPUT is not defined.");
        if (!STORAGE_CONTAINER_OUTPUT) throw new Error("STORAGE_CONTAINER_OUTPUT is not defined.");
        if (!STORAGE_CONTAINER_SCHEMAS) throw new Error("STORAGE_CONTAINER_SCHEMAS is not defined.");
        if (!STORAGE_SAS && !STORAGE_KEY) throw new Error("STORAGE_SAS or STORAGE_KEY must be defined.");
        if (context.log) context.log.verbose("validated succesfully.");

        // establish connections
        const input = new BlobHelper({
            account:   STORAGE_ACCOUNT,
            sas:       STORAGE_SAS,
            key:       STORAGE_KEY,
            container: STORAGE_CONTAINER_INPUT
        });
        bridgeLogs(input, context);
        const output = new BlobHelper({
            service:   input.service,
            container: STORAGE_CONTAINER_OUTPUT
        });
        bridgeLogs(output, context);
        const schema = new BlobHelper({
            service:   input.service,
            container: STORAGE_CONTAINER_SCHEMAS
        });
        bridgeLogs(schema, context);

        // get the message
        if (context.log) context.log.verbose(`getting the message from queue "processing"...`);
        const message: message = context.bindings.queue;
        if (context.log) context.log.verbose(`successfully got the message from queue "processing".`);

        // read schemas (with some parallelism)
        if (context.log) context.log.verbose(`getting schemas from "${STORAGE_CONTAINER_SCHEMAS}"...`);
        const schemaBlobs = await schema.list(/.+\.json$/g);
        const schemaFilenames = schemaBlobs.map(s => s.name);
        const schemas = await schema.loadFiles(schemaFilenames, "json");

        // read the input files
        if (context.log) context.log.verbose(`getting blobs from "${STORAGE_CONTAINER_INPUT}/${message.partition}"...`);
        const files = await input.loadFiles(message.filenames, "text");

        // batch up the rows so it can write more efficiently
        const batch: BlobHelperBatchWrite[] = [];

        // NOTE: no schemas are being applied

        // apply schemas
        for (const file of files) {
            const doc = new dom.DOMParser().parseFromString(file);
            for (const s of schemas) {
                if (xpath.select(s.identify, doc).length > 0) {
                    if (context.log) context.log.verbose(`schema identified as "${s.name}".`)
        
                    // extract the columns
                    const row: string[] = [];
                    for (const column of s.columns) {
                        const enclosure = column.enclosure || "";
                        const _default = column.default || "";
                        const value = xpath.select1(`string(${column.path})`, doc);
                        if (value) {
                            row.push(`${enclosure}${value}${enclosure}`);
                        } else {
                            row.push(`${enclosure}${_default}${enclosure}`);
                        }
                    }

                    // buffer the row
                    const filename = `${message.partition}/${s.filename}`;
                    let entry = batch.find(f => f.filename === filename);
                    if (!entry) {
                        entry = {
                            filename: filename,
                            content:  ""
                        };
                        batch.push(entry);
                    }
                    entry.content += row.join(",") + "\n";

                }
            }
        }

        // flush all the writes (with concurrency)
        await output.appendToBlobs(batch);

    } catch (error) {
        // TODO: also log to some permanent space
        if (context.log) context.log.error(error.stack);
    }

    // respond
    context.done();
    
}