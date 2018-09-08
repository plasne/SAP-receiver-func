"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const azs = __importStar(require("azure-storage"));
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
        // find out if anything is in the queue
        const service = azs.createQueueService(AZURE_WEB_JOBS_STORAGE);
        const status = await new Promise((resolve, reject) => {
            service.peekMessage("processing", (error, result) => {
                if (error)
                    reject(error);
                if (result) {
                    resolve("processing");
                }
                else {
                    resolve("done");
                }
            });
        });
        // respond with status
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        context.res.body = { status: status };
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
