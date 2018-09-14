
// includes
import { Context, HttpStatusCode } from "azure-functions-ts-essentials";
import QueueHelper from "../global/QueueHelper";

function bridgeLogs(helper: QueueHelper, context: Context) {
    if (context.log) {
        helper.events.on("info", msg => { if (context.log) context.log.info(msg) });
        helper.events.on("verbose", msg => { if (context.log) context.log.verbose(msg) });
        helper.events.on("error", msg => { if (context.log) context.log.error(msg) });
    }
}

// variables
const AZURE_WEB_JOBS_STORAGE: string | undefined = process.env.AzureWebJobsStorage;

// module
export async function run(context: Context) {
    try {

        // validate
        if (!AZURE_WEB_JOBS_STORAGE) throw new Error("AzureWebJobsStorage is not defined.");
        if (!context.req || !context.res) throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.originalUrl) throw new Error("The URL could not be determined.");

        // connect to the queue
        const queue = new QueueHelper({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            name:    "processing",
            encoder: "base64"
        });
        bridgeLogs(queue, context);

        // respond
        context.res.status = HttpStatusCode.OK;
        const hasMessages = await queue.hasMessages();
        if (hasMessages) {
            context.res.body = { status: "processing" };
        } else {
            context.res.body = { status: "done" };
        }

    } catch (error) {
        if (context.res) context.res.status = HttpStatusCode.InternalServerError;
        if (context.log) context.log.error(error.stack);
    }

    // respond
    context.done();
    
}