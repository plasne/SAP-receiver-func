// includes
import * as azs from 'azure-storage';
import * as util from 'util';
import AzureBlobOperation from './AzureBlobOperation';
import ReadableStream from './ReadableStream';
import { IStreamOptions } from './Stream';
import WriteableStream from './WriteableStream';

export interface IAzureBlobOptions {
    service?: azs.BlobService;
    connectionString?: string;
    account?: string;
    sas?: string;
    key?: string;
}

interface IAzureBlobStreams<T, U> {
    in: WriteableStream<T, AzureBlobOperation>;
    out: ReadableStream<any, U>;
}

export default class AzureBlob {
    public service: azs.BlobService;

    constructor(obj: IAzureBlobOptions) {
        // establish the service
        if (obj.service) {
            this.service = obj.service;
        } else if (obj.connectionString) {
            this.service = azs.createBlobService(obj.connectionString);
        } else if (obj.account && obj.sas) {
            const host = `https://${obj.account}.blob.core.windows.net`;
            this.service = azs.createBlobServiceWithSas(host, obj.sas);
        } else if (obj.account && obj.key) {
            this.service = azs.createBlobService(obj.account, obj.key);
        } else {
            throw new Error(
                `You must specify service, connectionString, account/sas, or account/key.`
            );
        }
    }

    public createBlockBlobFromText(
        container: string,
        filename: string,
        content: string
    ) {
        const createBlockBlobFromText: (
            container: string,
            blob: string,
            text: string
        ) => Promise<void> = util
            .promisify(azs.BlobService.prototype.createBlockBlobFromText)
            .bind(this.service);
        return createBlockBlobFromText(container, filename, content);
    }

    // create or replace an append blob
    public async createOrReplaceAppendBlob(
        container: string,
        filename: string,
        content?: string
    ) {
        const createOrReplaceAppendBlob: (
            container: string,
            blob: string
        ) => Promise<void> = util
            .promisify(azs.BlobService.prototype.createOrReplaceAppendBlob)
            .bind(this.service);
        if (content) {
            await createOrReplaceAppendBlob(container, filename);
            return this.appendToBlob(container, filename, content);
        } else {
            return createOrReplaceAppendBlob(container, filename);
        }
    }

    // append content
    public appendToBlob(container: string, filename: string, content: string) {
        const appendBlockFromText: (
            container: string,
            blob: string,
            content: string
        ) => Promise<azs.BlobService.BlobResult> = util
            .promisify(azs.BlobService.prototype.appendBlockFromText)
            .bind(this.service);
        return appendBlockFromText(container, filename, content);
    }

    // load a file
    public async load(container: string, filename: string) {
        const getBlobToText: (
            container: string,
            blob: string
        ) => Promise<string> = util
            .promisify(azs.BlobService.prototype.getBlobToText)
            .bind(this.service);
        const raw = await getBlobToText(container, filename);
        return raw;
    }

    public streams<In = AzureBlobOperation, Out = any>(): IAzureBlobStreams<
        In,
        Out
    >;

    public streams<In = AzureBlobOperation, Out = any>(
        inOptions: IStreamOptions<In, AzureBlobOperation>,
        outOptions: IStreamOptions<any, Out>
    ): IAzureBlobStreams<In, Out>;

    public streams<In = AzureBlobOperation, Out = any>(): IAzureBlobStreams<
        In,
        Out
    > {
        // get arguments
        const inOptions: IStreamOptions<In, AzureBlobOperation> =
            arguments[0] || {};
        const outOptions: IStreamOptions<any, Out> = arguments[1] || {};

        // create the streams
        const streams: IAzureBlobStreams<In, Out> = {
            in: new WriteableStream<In, AzureBlobOperation>(inOptions),
            out: new ReadableStream<any, Out>(outOptions)
        };

        // produce promises to commit the operations
        streams.out
            .process(streams.in, () => {
                // perform specified operation
                const op = streams.in.buffer.shift();
                if (op) {
                    let type = op.type;
                    if (op.type === 'list' && op.prefix) {
                        type = 'listWithPrefix';
                    }
                    switch (type) {
                        case 'append':
                            if (op.filename) {
                                return this.appendToBlob(
                                    op.container,
                                    op.filename,
                                    op.content || ''
                                )
                                    .then(result => {
                                        streams.out.emit('success', result, op);
                                        op.resolve(result);
                                    })
                                    .catch(error => {
                                        streams.out.emit('error', error, op);
                                        op.reject(error);
                                    });
                            }
                            break;
                        case 'createAppend':
                            if (op.filename) {
                                return this.createOrReplaceAppendBlob(
                                    op.container,
                                    op.filename,
                                    op.content
                                )
                                    .then(result => {
                                        streams.out.emit('success', result, op);
                                        op.resolve(result);
                                    })
                                    .catch(error => {
                                        streams.out.emit('error', error, op);
                                        op.reject(error);
                                    });
                            }
                            break;
                        case 'load':
                            if (op.filename) {
                                return this.load(op.container, op.filename)
                                    .then(result => {
                                        streams.out.push(result, op);
                                        op.resolve(result);
                                    })
                                    .catch(error => {
                                        streams.out.emit('error', error, op);
                                        op.reject(error);
                                    });
                            }
                            break;
                        case 'list':
                            return new Promise((resolve, reject) => {
                                return this.service.listBlobsSegmented(
                                    op.container,
                                    op.token,
                                    (error, result) => {
                                        if (!error) {
                                            for (const entity of result.entries) {
                                                streams.out.push(entity, op);
                                            }
                                            if (result.continuationToken) {
                                                op.token =
                                                    result.continuationToken;
                                                streams.in.buffer.push(op);
                                            } else {
                                                op.resolve();
                                            }
                                            resolve();
                                        } else {
                                            streams.out.emit(
                                                'error',
                                                error,
                                                op
                                            );
                                            op.reject(error);
                                            reject(error);
                                        }
                                    }
                                );
                            });
                        case 'listWithPrefix':
                            return new Promise((resolve, reject) => {
                                return this.service.listBlobsSegmentedWithPrefix(
                                    op.container,
                                    op.prefix || '',
                                    op.token,
                                    (error, result) => {
                                        if (!error) {
                                            for (const entity of result.entries) {
                                                streams.out.push(entity, op);
                                            }
                                            if (result.continuationToken) {
                                                op.token =
                                                    result.continuationToken;
                                                streams.in.buffer.push(op);
                                            } else {
                                                op.resolve();
                                            }
                                            resolve();
                                        } else {
                                            streams.out.emit(
                                                'error',
                                                error,
                                                op
                                            );
                                            op.reject(error);
                                            reject(error);
                                        }
                                    }
                                );
                            });
                    }
                }

                // nothing else to do
                return null;
            })
            .catch(error => {
                streams.out.emit('error', error);
            });

        return streams;
    }

    public stream<In = AzureBlobOperation, Out = any>(
        operations: In | In[],
        inOptions?: IStreamOptions<In, AzureBlobOperation>,
        outOptions?: IStreamOptions<any, Out>
    ): ReadableStream<any, Out> {
        // start the stream
        const streams = this.streams<In, Out>(
            inOptions || {},
            outOptions || {}
        );

        // push the operations
        if (Array.isArray(operations)) {
            for (const operation of operations) {
                streams.in.push(operation);
            }
        } else {
            streams.in.push(operations);
        }

        // end the input stream
        streams.in.end();
        return streams.out;
    }

    public process<In = AzureBlobOperation, Out = any>(
        operations: In | In[],
        inOptions?: IStreamOptions<In, AzureBlobOperation>,
        outOptions?: IStreamOptions<any, Out>
    ): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            try {
                // start commit
                const stream = this.stream(operations, inOptions, outOptions);

                // resolve when done
                stream.once('end', () => {
                    resolve();
                });
            } catch (error) {
                reject(error);
            }
        });
    }

    public loadAsStream<Out = string>(
        container: string,
        prefix?: string,
        outOptions?: IStreamOptions<string, Out>
    ): ReadableStream<string, Out> {
        const loader = this.streams<azs.BlobService.BlobResult, Out>(
            {
                transform: data =>
                    new AzureBlobOperation(container, 'load', data.name)
            },
            outOptions || {}
        );
        const listOperation = new AzureBlobOperation(container, 'list');
        if (prefix) {
            listOperation.type = 'listWithPrefix';
            listOperation.prefix = prefix;
        }
        this.stream<AzureBlobOperation, azs.BlobService.BlobResult>(
            listOperation
        ).pipe(loader.in);
        return loader.out;
    }

    public listAsStream<Out = azs.BlobService.BlobResult>(
        container: string,
        prefix?: string,
        outOptions?: IStreamOptions<azs.BlobService.BlobResult, Out>
    ): ReadableStream<azs.BlobService.BlobResult, Out> {
        const listOperation = new AzureBlobOperation(container, 'list');
        if (prefix) {
            listOperation.type = 'listWithPrefix';
            listOperation.prefix = prefix;
        }
        return this.stream<AzureBlobOperation, Out>(
            listOperation,
            {},
            outOptions
        );
    }

    // create the container if it doesn't already exist
    public createContainerIfNotExists(container: string) {
        const createContainerIfNotExists: (
            container: string
        ) => Promise<azs.BlobService.ContainerResult> = util
            .promisify(azs.BlobService.prototype.createContainerIfNotExists)
            .bind(this.service);
        return createContainerIfNotExists(container);
    }
}
