
// includes
import { Context, HttpStatusCode } from "azure-functions-ts-essentials";
import * as azs from "azure-storage";

// variables
const AZURE_WEB_JOBS_STORAGE: string | undefined = process.env.AzureWebJobsStorage;

// module
export async function run(context: Context) {
    try {

        // validate
        if (!AZURE_WEB_JOBS_STORAGE) throw new Error("AzureWebJobsStorage is not defined.");
        if (!context.req || !context.res) throw new Error("Request/Response must be defined in bindings.");
        if (!context.req.originalUrl) throw new Error("The URL could not be determined.");

        // find out if anything is in the queue
        const service = azs.createQueueService(AZURE_WEB_JOBS_STORAGE);
        const status = await new Promise<string>((resolve, reject) => {
            service.peekMessage("processing", (error, result) => {
                if (error) reject(error);
                if (result) {
                    resolve("processing");
                } else {
                    resolve("done");
                }
            });
        });

        // respond with status
        context.res.status = HttpStatusCode.OK;
        context.res.body = { status: status };

    } catch (error) {
        if (context.res) context.res.status = HttpStatusCode.InternalServerError;
        if (context.log) context.log.error(error.stack);
    }

    // respond
    context.done();
    
}