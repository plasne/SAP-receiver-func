// FUNCTION: start()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// INPUT:    ?partition=<name_of_the_partition_to_process>
// PURPOSE:  This function:
//           1. creates a CSV file with headers but no rows for each schema definition
//           2. creates a CSV file to hold errors
//           3. reads all blobs from the specified partition
//           4. breaks the list of blobs into chunks of size FILES_PER_MESSAGE
//           5. enqueues the messages for the processor()

// notes:
//  "azure-fucntions-ts-essentials" required "npm install -D types/node"
//  "es6-promise-pool" required "dom" being included in the library
//  host.json must use "Trace" to see "verbose" logs

// includes
import { Context, HttpStatusCode } from 'azure-functions-ts-essentials';
import AzureBlob from '../global/AzureBlob';
import AzureBlobOperation from '../global/AzureBlobOperation';
import AzureQueue from '../global/AzureQueue';
import AzureQueueOperation from '../global/AzureQueueOperation';
import { message as queueMessage } from '../global/custom';

// function to cast to a number or use a default if thats not possible
function valueOrDefault(value: string | undefined, dflt: number) {
    if (value) {
        const i = parseInt(value, 10);
        return Number.isNaN(i) ? dflt : i;
    } else {
        return dflt;
    }
}

// variables
const AZURE_WEB_JOBS_STORAGE: string | undefined =
    process.env.AzureWebJobsStorage;
const STORAGE_ACCOUNT: string | undefined = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT: string | undefined =
    process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_CONTAINER_OUTPUT: string | undefined =
    process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS: string | undefined =
    process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS: string | undefined = process.env.STORAGE_SAS;
const STORAGE_KEY: string | undefined = process.env.STORAGE_KEY;
const FILES_PER_MESSAGE: number = valueOrDefault(
    process.env.FILES_PER_MESSAGE,
    10
);

// module
export async function run(context: Context) {
    const start = new Date();
    try {
        // validate
        if (!AZURE_WEB_JOBS_STORAGE) {
            throw new Error('AzureWebJobsStorage is not defined.');
        }
        if (!STORAGE_ACCOUNT) {
            throw new Error('STORAGE_ACCOUNT is not defined.');
        }
        if (!STORAGE_CONTAINER_INPUT) {
            throw new Error('STORAGE_CONTAINER_INPUT is not defined.');
        }
        if (!STORAGE_CONTAINER_OUTPUT) {
            throw new Error('STORAGE_CONTAINER_OUTPUT is not defined.');
        }
        if (!STORAGE_CONTAINER_SCHEMAS) {
            throw new Error('STORAGE_CONTAINER_SCHEMAS is not defined.');
        }
        if (!STORAGE_SAS && !STORAGE_KEY) {
            throw new Error('STORAGE_SAS or STORAGE_KEY must be defined.');
        }
        if (!context.req || !context.res) {
            throw new Error('Request/Response must be defined in bindings.');
        }
        if (!context.req.query || !context.req.query.partition) {
            throw new Error('The partition could not be determined.');
        }
        if (context.log) context.log.verbose('validated succesfully');
        const partition = context.req.query.partition;

        // count errors
        let errors = 0;

        // establish connections
        const blob = new AzureBlob({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS
        });
        const queue = new AzureQueue({
            connectionString: AZURE_WEB_JOBS_STORAGE,
            encoder: 'base64'
        });

        // create an output stream for writing the files
        const output = blob.streams<AzureBlobOperation, any>(
            {},
            {
                processAfter: blob.createContainerIfNotExists(
                    STORAGE_CONTAINER_OUTPUT
                )
            }
        );
        output.out.on('error', (error: Error) => {
            if (context.log) context.log.error(error);
            errors++;
        });

        // create a file to hold errors
        output.in.push(
            new AzureBlobOperation(
                STORAGE_CONTAINER_OUTPUT,
                'createAppend',
                `${partition}/errors.txt`
            )
        );

        // as schemas are loaded, create the output files for each
        blob.loadAsStream(STORAGE_CONTAINER_SCHEMAS)
            .on('data', (data: string, metadata: any) => {
                const obj = JSON.parse(data);
                if (context.log) {
                    context.log.info(`schema "${metadata.filename}" loaded.`);
                }

                // create file for each schema with header
                const headers: string[] = [];
                for (const column of obj.columns) {
                    headers.push(column.header);
                }
                output.in.push(
                    new AzureBlobOperation(
                        STORAGE_CONTAINER_OUTPUT,
                        'createAppend',
                        `${partition}/${obj.filename}`,
                        headers.join(',') + '\n'
                    )
                );
            })
            .on('end', () => {
                output.in.end();
            })
            .on('error', error => {
                if (context.log) context.log.error(error);
                errors++;
            });

        // create an output stream for queue messages
        const enqueuer = queue.streams<AzureQueueOperation, string>(
            {},
            {
                processAfter: queue.createQueueIfNotExists('processing')
            }
        );
        enqueuer.out.on('error', (error: Error) => {
            if (context.log) context.log.error(error);
            errors++;
        });

        // define the dispatch process
        let batch: string[] = [];
        let queued = 0;
        const dispatch = () => {
            const message: queueMessage = {
                filenames: batch,
                partition
            };
            queued += batch.length;
            enqueuer.in.push(
                new AzureQueueOperation('processing', 'enqueue', message)
            );
            batch = [];
        };

        // start fetching blobs
        const filenames = blob
            .listAsStream<string>(STORAGE_CONTAINER_INPUT, partition, {
                transform: data => {
                    if (/.+\.xml$/g.test(data.name)) {
                        return data.name;
                    } else {
                        return null;
                    }
                }
            })
            .on('data', (filename: string) => {
                batch.push(filename);
                if (batch.length >= FILES_PER_MESSAGE) dispatch();
            })
            .on('end', () => {
                dispatch();
                enqueuer.in.end();
            })
            .on('error', error => {
                if (context.log) context.log.error(error);
                errors++;
            });

        // monitor what is happening
        const statusTimer = setInterval(() => {
            if (context.log) {
                context.log.info(
                    `${queued} blobs queued for processing, ${
                        filenames.buffer.length
                    } in the buffer...`
                );
            }
        }, 1000);

        // wait for all operations to finish
        await Promise.all([output.out.waitForEnd(), enqueuer.out.waitForEnd()]);

        // done
        if (statusTimer) clearInterval(statusTimer);
        if (context.log) {
            context.log.info(
                `completed with ${queued} blobs queued for processing.`
            );
        }

        // respond with status
        if (errors > 0) throw new Error('errors were found in the streams');
        context.res.status = HttpStatusCode.OK;
        context.res.body = { status: 'started' };
    } catch (error) {
        if (context.res) {
            context.res.status = HttpStatusCode.InternalServerError;
        }
        if (context.log) context.log.error(error.stack);
    }

    // respond
    const elapsedInMin = (new Date().valueOf() - start.valueOf()) / 1000 / 60;
    if (context.log) {
        context.log.verbose(
            `start() took ${elapsedInMin.toFixed(2)} minutes to complete.`
        );
    }
}
