// FUNCTION: processor()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function:
//           1. loads all schemas
//           2. loads the set of files it was assigned from blob storage
//           3. attempts to apply each schema to each file generating rows
//           4. all rows updates are flushed to storage

// includes
import { Context } from 'azure-functions-ts-essentials';
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

        // establish connections
        const blob = new AzureBlob({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS
        });

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

        // batch up the rows so it can write more efficiently
        const batch: AzureBlobOperation[] = [];

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
            .on('data', (data: string, op: any) => {
                if (context.log) {
                    context.log.info(`looking for schemas for ${op.filename}`);
                }
                const doc = new dom.DOMParser().parseFromString(data);
                for (const s of schemas.buffer) {
                    if (xpath.select(s.identify, doc).length > 0) {
                        if (context.log) {
                            context.log.info(
                                `schema identified as "${s.name}"`
                            );
                        }

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
                                row.push(`${enclosure}${value}${enclosure}`);
                            } else {
                                row.push(`${enclosure}${dflt}${enclosure}`);
                            }
                        }

                        // buffer the row
                        const filename = `${message.partition}/${s.filename}`;
                        let entry = batch.find(f => f.filename === filename);
                        if (!entry) {
                            entry = new AzureBlobOperation(
                                STORAGE_CONTAINER_OUTPUT,
                                'append',
                                filename
                            );
                            batch.push(entry);
                        }
                        entry.content += row.join(',') + '\n';
                    }
                }
            })
            .on('error', error => {
                if (context.log) context.log.error(error);
            });

        // wait for files to be fully processed
        await loader.waitForEnd();

        // flush all the writes (with concurrency)
        const writer = blob.stream<AzureBlobOperation, any>(batch);
        await writer.waitForEnd();
    } catch (error) {
        // TODO: also log to some permanent space
        if (context.log) context.log.error(error.stack);
    }
}
