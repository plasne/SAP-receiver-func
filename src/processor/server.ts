// FUNCTION: processor()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function:
//           1. loads all schemas
//           2. loads the set of files it was assigned from blob storage
//           3. attempts to apply each schema to each file generating rows
//           4. all rows updates are flushed to storage

// includes
import { Context } from 'azure-functions-ts-essentials';
import * as http from 'http';
import * as https from 'https';
import * as dom from 'xmldom';
import * as xpath from 'xpath';
import AzureBlob from '../global/AzureBlob';
import AzureBlobOperation from '../global/AzureBlobOperation';
import { message as queueMessage } from '../global/custom';

// variables
const STORAGE_ACCOUNT: string | undefined = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT: string | undefined =
    process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_CONTAINER_OUTPUT: string | undefined =
    process.env.STORAGE_CONTAINER_OUTPUT;
const STORAGE_CONTAINER_SCHEMAS: string | undefined =
    process.env.STORAGE_CONTAINER_SCHEMAS;
const STORAGE_SAS: string | undefined = process.env.STORAGE_SAS;
const STORAGE_KEY: string | undefined = process.env.STORAGE_KEY;

// modify the agents
const httpAgent: any = http.globalAgent;
httpAgent.keepAlive = true;
httpAgent.maxSockets = 30;
const httpsAgent: any = https.globalAgent;
httpsAgent.keepAlive = true;
httpsAgent.maxSockets = 30;

// module
export async function run(context: Context) {
    try {
        // validate
        if (context.log) context.log.verbose('validating configuration...');
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
        if (context.log) context.log.verbose('validated succesfully.');

        // get the message
        if (context.log) {
            context.log.info(`getting the message from queue "processing"...`);
        }
        const message: queueMessage = context.bindings.queue;
        if (context.log) {
            context.log.info(
                `successfully got the message from queue "processing" (${
                    message.filenames.length
                } files).`
            );
        }

        // establish connections
        const blob = new AzureBlob({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS,
            useGlobalAgent: true
        });

        // batch up the rows so it can write more efficiently
        const batch: AzureBlobOperation[] = [];

        // define error function
        const raise = (msg: string, partition?: string, filename?: string) => {
            const formatted = `${new Date().toISOString()} ${
                partition ? '[' + partition + ']' : ''
            } ${filename ? '[' + filename + ']' : ''} ${msg}\n`;
            batch.push(
                new AzureBlobOperation(
                    STORAGE_CONTAINER_OUTPUT,
                    'append',
                    `${message.partition}/errors.txt`,
                    formatted
                )
            );
        };

        // start processing
        try {
            // load all schemas
            if (context.log) {
                context.log.info(
                    `getting schemas from "${STORAGE_CONTAINER_SCHEMAS}"...`
                );
            }
            const schemas = blob
                .loadAsStream<any>(STORAGE_CONTAINER_SCHEMAS, undefined, {
                    transform: data => JSON.parse(data)
                })
                .on('error', error => {
                    if (context.log) context.log.error(error);
                });
            await schemas.waitForEnd();

            // start processing files
            if (context.log) {
                context.log.info(
                    `getting blobs from "${STORAGE_CONTAINER_INPUT}/${
                        message.partition
                    }"...`
                );
            }
            const loader = blob
                .stream<string, string>(message.filenames, {
                    transform: (filename: string) =>
                        new AzureBlobOperation(
                            STORAGE_CONTAINER_INPUT,
                            'load',
                            filename
                        )
                })
                .on('data', (data: string, op: AzureBlobOperation) => {
                    // start logging
                    let schemasFound = 0;
                    if (context.log) {
                        context.log.info(
                            `looking for schemas for ${op.filename}...`
                        );
                    }

                    // parse as XML
                    let doc: Document | undefined;
                    try {
                        doc = new dom.DOMParser().parseFromString(data);
                    } catch (error) {
                        if (context.log) {
                            context.log.info(
                                'the file was not properly formatted XML, it will be skipped.'
                            );
                        }
                        raise(
                            'the file was not properly formatted XML, it will be skipped.',
                            message.partition,
                            op.filename
                        );
                    }

                    // see which schemas match
                    if (doc) {
                        for (const s of schemas.buffer) {
                            if (xpath.select(s.identify, doc).length > 0) {
                                // identify as a found schema
                                if (context.log) {
                                    context.log.info(
                                        `schema identified as "${s.name}".`
                                    );
                                }
                                schemasFound++;

                                // extract the columns
                                const row: string[] = [];
                                for (const column of s.columns) {
                                    const enclosure = column.enclosure || '';
                                    const dflt = column.default || '';
                                    const value = xpath.select1(
                                        `string(${column.path})`,
                                        doc
                                    );
                                    if (value) {
                                        row.push(
                                            `${enclosure}${value}${enclosure}`
                                        );
                                    } else {
                                        row.push(
                                            `${enclosure}${dflt}${enclosure}`
                                        );
                                    }
                                }

                                // buffer the row
                                const filename = `${message.partition}/${
                                    s.filename
                                }`;
                                let entry = batch.find(
                                    f => f.filename === filename
                                );
                                if (!entry) {
                                    entry = new AzureBlobOperation(
                                        STORAGE_CONTAINER_OUTPUT,
                                        'append',
                                        filename,
                                        ''
                                    );
                                    batch.push(entry);
                                }
                                entry.content += row.join(',') + '\n';
                            }
                        }

                        // raise error if no schemas were found
                        if (schemasFound < 1) {
                            if (context.log) {
                                context.log.error(
                                    'no schemas were found that match this file.'
                                );
                            }
                            raise(
                                'no schemas were found that match this file.',
                                message.partition,
                                op.filename
                            );
                        }
                    }
                })
                .on('error', (error: Error, op: AzureBlobOperation) => {
                    if (context.log) context.log.error(error);
                    raise(error.message, message.partition, op.filename);
                });

            // wait for files to be fully processed
            await loader.waitForEnd();
        } catch (error) {
            if (context.log) context.log.error(error.stack);
            raise(error.stack);
        }

        // flush all the writes
        if (context.log) {
            context.log.info(`flushing ${batch.length} write(s)...`);
        }
        const writer = blob.stream<AzureBlobOperation, any>(batch);
        await writer.waitForEnd();
        if (context.log) {
            context.log.info(`all writes flushed.`);
        }
    } catch (error) {
        if (context.log) context.log.error(error.stack);
        process.exit(1);
    }
}
