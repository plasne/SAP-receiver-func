"use strict";
// FUNCTION: status()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function simply returns "processing" if there are still items in the queue, or
//           "done" if there are not.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const AzureQueue_1 = __importDefault(require("../global/AzureQueue"));
// variables
const AZURE_WEB_JOBS_STORAGE = process.env.AzureWebJobsStorage;
// module
async function run(context) {
    try {
        // validate
        if (!AZURE_WEB_JOBS_STORAGE) {
            throw new Error('AzureWebJobsStorage is not defined.');
        }
        if (!context.req || !context.res) {
            throw new Error('Request/Response must be defined in bindings.');
        }
        if (!context.req.originalUrl) {
            throw new Error('The URL could not be determined.');
        }
        // connect to the queue
        const queue = new AzureQueue_1.default({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            encoder: 'base64'
        });
        // respond
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        const hasMessages = await queue.hasMessages('processing');
        if (hasMessages) {
            context.res.body = { status: 'processing' };
        }
        else {
            context.res.body = { status: 'done' };
        }
    }
    catch (error) {
        if (context.res) {
            context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.InternalServerError;
        }
        if (context.log)
            context.log.error(error.stack);
    }
}
exports.run = run;
