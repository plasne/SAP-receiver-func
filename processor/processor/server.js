"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
//import { message } from "../global/custom";
//import BlobHelper from "../global/BlobHelper";
//import * as azs from "azure-storage";
// variables
//const AZURE_WEB_JOBS_STORAGE: string | undefined = process.env.AzureWebJobsStorage;
// module
async function run(context) {
    try {
        // decode the message
        //const raw: string = context.bindings.queue;
        //const message: message = JSON.parse(raw);
        // get all schemas
        // load each file
        //for (const filename of message.filenames) {
        //}
    }
    catch (error) {
        //if (context.res) context.res.status = HttpStatusCode.InternalServerError;
        //if (context.log) context.log.error(error.stack);
    }
    // respond
    context.done();
}
exports.run = run;
