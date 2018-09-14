"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const QueueHelper_1 = __importDefault(require("../global/QueueHelper"));
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
// module
async function run(context) {
    try {
        // validate
        if (!AZURE_WEB_JOBS_STORAGE)
            throw new Error("AzureWebJobsStorage is not defined.");
        if (!context.req || !context.res)
            throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.originalUrl)
            throw new Error("The URL could not be determined.");
        // connect to the queue
        const queue = new QueueHelper_1.default({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            name: "processing",
            encoder: "base64"
        });
        bridgeLogs(queue, context);
        // respond
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        const hasMessages = await queue.hasMessages();
        if (hasMessages) {
            context.res.body = { status: "processing" };
        }
        else {
            context.res.body = { status: "done" };
        }
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
