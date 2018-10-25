// includes
import * as azs from 'azure-storage';
import * as util from 'util';
import AzureQueueOperation from './AzureQueueOperation';
import ReadableStream from './ReadableStream';
import { IStreamOptions } from './Stream';
import WriteableStream from './WriteableStream';

export type encoders = 'base64' | 'xml' | 'binary';

export interface IAzureQueueOptions {
    service?: azs.QueueService;
    useGlobalAgent?: boolean;
    connectionString?: string;
    account?: string;
    sas?: string;
    key?: string;
    encoder?: encoders;
}

interface IAzureQueueStreams<T, U> {
    in: WriteableStream<T, AzureQueueOperation>;
    out: ReadableStream<string, U>;
}

export default class AzureQueue {
    public service: azs.QueueService;

    constructor(obj: IAzureQueueOptions) {
        // establish the service
        if (obj.service) {
            this.service = obj.service;
        } else if (obj.connectionString) {
            this.service = azs.createQueueService(obj.connectionString);
            if (obj.encoder) this.encoder = obj.encoder;
            if (obj.useGlobalAgent) this.service.enableGlobalHttpAgent = true;
        } else if (obj.account && obj.sas) {
            const host = `https://${obj.account}.queue.core.windows.net`;
            this.service = azs.createQueueServiceWithSas(host, obj.sas);
            if (obj.encoder) this.encoder = obj.encoder;
            if (obj.useGlobalAgent) this.service.enableGlobalHttpAgent = true;
        } else if (obj.account && obj.key) {
            this.service = azs.createQueueService(obj.account, obj.key);
            if (obj.encoder) this.encoder = obj.encoder;
            if (obj.useGlobalAgent) this.service.enableGlobalHttpAgent = true;
        } else {
            throw new Error(
                'You must specify service, connectionString, account/sas, or account/key.'
            );
        }
    }

    /** Returns *true* if there are any messages in the queue. */
    public async hasMessages(queue: string) {
        const getQueueMetadata: (
            queue: string
        ) => Promise<azs.QueueService.QueueResult> = util
            .promisify(azs.QueueService.prototype.getQueueMetadata)
            .bind(this.service);
        const result = await getQueueMetadata(queue);
        if (result.approximateMessageCount == null) {
            return true; // it is safer to assume there could be
        }
        return result.approximateMessageCount > 0;
    }

    public streams<
        In = AzureQueueOperation,
        Out = string
    >(): IAzureQueueStreams<In, Out>;

    public streams<In = AzureQueueOperation, Out = AzureQueueOperation>(
        inOptions: IStreamOptions<In, AzureQueueOperation>,
        outOptions: IStreamOptions<string, Out>
    ): IAzureQueueStreams<In, Out>;

    public streams<In = string, Out = string>(): IAzureQueueStreams<In, Out> {
        // get arguments
        const inOptions: IStreamOptions<In, AzureQueueOperation> =
            arguments[0] || {};
        const outOptions: IStreamOptions<string, Out> = arguments[1] || {};

        // create the streams
        const streams: IAzureQueueStreams<In, Out> = {
            in: new WriteableStream<In, AzureQueueOperation>(inOptions),
            out: new ReadableStream<string, Out>(outOptions)
        };

        // produce promises to perform the operations
        streams.out
            .process(streams.in, () => {
                // perform specified operation
                const op = streams.in.buffer.shift();
                if (op) {
                    switch (op.direction) {
                        case 'dequeue':
                            return this.dequeueMessages(op.queue, op.count)
                                .then(result => {
                                    for (const pkg of result) {
                                        if (pkg.messageText) {
                                            streams.out.push(
                                                pkg.messageText,
                                                op
                                            );
                                        }
                                    }
                                    op.resolve(result);
                                })
                                .catch(error => {
                                    streams.out.emit('error', error);
                                    op.reject(error);
                                });
                        case 'enqueue':
                            if (op.message) {
                                return this.enqueueMessage(op.queue, op.message)
                                    .then(result => {
                                        streams.out.emit('success', result);
                                        op.resolve(result);
                                    })
                                    .catch(error => {
                                        streams.out.emit('error', error);
                                        op.reject(error);
                                    });
                            }
                            break;
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

    public stream<In = AzureQueueOperation, Out = string>(
        operations: In | In[],
        inOptions?: IStreamOptions<In, AzureQueueOperation>,
        outOptions?: IStreamOptions<string, Out>
    ): ReadableStream<string, Out> {
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

    public process<In = AzureQueueOperation, Out = string>(
        operations: In | In[],
        inOptions?: IStreamOptions<In, AzureQueueOperation>,
        outOptions?: IStreamOptions<string, Out>
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

    public enqueueMessages(queue: string, messages: string[] | object[]) {
        const operations: AzureQueueOperation[] = [];
        for (const message of messages) {
            const toString =
                typeof message === 'object' ? JSON.stringify(message) : message;
            operations.push(
                new AzureQueueOperation(queue, 'enqueue', toString)
            );
        }
        return this.process(operations);
    }

    // add a single message to the queue
    public enqueueMessage(queue: string, message: string | object) {
        const createMessage: (
            queue: string,
            message: string
        ) => Promise<azs.QueueService.QueueMessageResult> = util
            .promisify(azs.QueueService.prototype.createMessage)
            .bind(this.service);
        const toString =
            typeof message === 'object' ? JSON.stringify(message) : message;
        return createMessage(queue, toString);
    }

    public dequeueMessages(queue: string, count: number = 1) {
        const getMessages: (
            queue: string,
            options: azs.QueueService.GetMessagesRequestOptions
        ) => Promise<azs.QueueService.QueueMessageResult[]> = util
            .promisify(azs.QueueService.prototype.getMessages)
            .bind(this.service);
        return getMessages(queue, {
            numOfMessages: count
        });
    }

    /** A Promise to create the queue if it doesn't exist. */
    public createQueueIfNotExists(queue: string) {
        const createQueueIfNotExists: (
            queue: string
        ) => Promise<azs.QueueService.QueueResult> = util
            .promisify(azs.QueueService.prototype.createQueueIfNotExists)
            .bind(this.service);
        return createQueueIfNotExists(queue);
    }

    /** Specify the encoding method ("base64" | "xml" | "binary"). */
    public set encoder(option: encoders) {
        switch (option) {
            case 'base64':
                this.service.messageEncoder = new azs.QueueMessageEncoder.TextBase64QueueMessageEncoder();
                break;
            case 'xml':
                this.service.messageEncoder = new azs.QueueMessageEncoder.TextXmlQueueMessageEncoder();
                break;
            case 'binary':
                this.service.messageEncoder = new azs.QueueMessageEncoder.BinaryBase64QueueMessageEncoder();
                break;
        }
    }
}
