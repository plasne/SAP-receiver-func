// FUNCTION: status()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function simply returns "processing" if there are still items in the queue, or
//           "done" if there are not.

// includes
import { Context, HttpStatusCode } from 'azure-functions-ts-essentials';
import AzureQueue from '../global/AzureQueue';

// variables
const AZURE_WEB_JOBS_STORAGE: string | undefined =
    process.env.AzureWebJobsStorage;

// module
export async function run(context: Context) {
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
        const queue = new AzureQueue({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            encoder: 'base64'
        });

        // respond
        context.res.status = HttpStatusCode.OK;
        const hasMessages = await queue.hasMessages('processing');
        if (hasMessages) {
            context.res.body = { status: 'processing' };
        } else {
            context.res.body = { status: 'done' };
        }
    } catch (error) {
        if (context.res) {
            context.res.status = HttpStatusCode.InternalServerError;
        }
        if (context.log) context.log.error(error.stack);
    }
}
