"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
//import * as azs from "azure-storage";
// variables
//const AZURE_WEB_JOBS_STORAGE: string | undefined = process.env.AzureWebJobsStorage;
// module
async function run(context) {
    try {
        const raw = context.bindings.queue;
        message: message = JSON.parse(raw);
    }
    catch (error) {
        //if (context.res) context.res.status = HttpStatusCode.InternalServerError;
        //if (context.log) context.log.error(error.stack);
    }
    // respond
    context.done();
}
exports.run = run;
