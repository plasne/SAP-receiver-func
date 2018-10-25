"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const azs = __importStar(require("azure-storage"));
const util = __importStar(require("util"));
const AzureBlobOperation_1 = __importDefault(require("./AzureBlobOperation"));
const ReadableStream_1 = __importDefault(require("./ReadableStream"));
const WriteableStream_1 = __importDefault(require("./WriteableStream"));
class AzureBlob {
    constructor(obj) {
        // establish the service
        if (obj.service) {
            this.service = obj.service;
        }
        else if (obj.connectionString) {
            this.service = azs.createBlobService(obj.connectionString);
        }
        else if (obj.account && obj.sas) {
            const host = `https://${obj.account}.blob.core.windows.net`;
            this.service = azs.createBlobServiceWithSas(host, obj.sas);
        }
        else if (obj.account && obj.key) {
            this.service = azs.createBlobService(obj.account, obj.key);
        }
        else {
            throw new Error(`You must specify service, connectionString, account/sas, or account/key.`);
        }
    }
    createBlockBlobFromText(container, filename, content) {
        const createBlockBlobFromText = util
            .promisify(azs.BlobService.prototype.createBlockBlobFromText)
            .bind(this.service);
        return createBlockBlobFromText(container, filename, content);
    }
    // create or replace an append blob
    async createOrReplaceAppendBlob(container, filename, content) {
        const createOrReplaceAppendBlob = util
            .promisify(azs.BlobService.prototype.createOrReplaceAppendBlob)
            .bind(this.service);
        if (content) {
            await createOrReplaceAppendBlob(container, filename);
            return this.appendToBlob(container, filename, content);
        }
        else {
            return createOrReplaceAppendBlob(container, filename);
        }
    }
    // append content
    appendToBlob(container, filename, content) {
        const appendBlockFromText = util
            .promisify(azs.BlobService.prototype.appendBlockFromText)
            .bind(this.service);
        return appendBlockFromText(container, filename, content);
    }
    // load a file
    async load(container, filename) {
        const getBlobToText = util
            .promisify(azs.BlobService.prototype.getBlobToText)
            .bind(this.service);
        const raw = await getBlobToText(container, filename);
        return raw;
    }
    streams() {
        // get arguments
        const inOptions = arguments[0] || {};
        const outOptions = arguments[1] || {};
        // create the streams
        const streams = {
            in: new WriteableStream_1.default(inOptions),
            out: new ReadableStream_1.default(outOptions)
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
                            return this.appendToBlob(op.container, op.filename, op.content || '')
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
                    case 'createAppend':
                        if (op.filename) {
                            return this.createOrReplaceAppendBlob(op.container, op.filename, op.content)
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
                    case 'load':
                        if (op.filename) {
                            return this.load(op.container, op.filename)
                                .then(result => {
                                streams.out.push(result, op);
                                op.resolve(result);
                            })
                                .catch(error => {
                                streams.out.emit('error', error);
                                op.reject(error);
                            });
                        }
                        break;
                    case 'list':
                        return new Promise((resolve, reject) => {
                            return this.service.listBlobsSegmented(op.container, op.token, (error, result) => {
                                if (!error) {
                                    for (const entity of result.entries) {
                                        streams.out.push(entity, op);
                                    }
                                    if (result.continuationToken) {
                                        op.token =
                                            result.continuationToken;
                                        streams.in.buffer.push(op);
                                    }
                                    else {
                                        op.resolve();
                                    }
                                    resolve();
                                }
                                else {
                                    streams.out.emit('error', error);
                                    op.reject(error);
                                    reject(error);
                                }
                            });
                        });
                    case 'listWithPrefix':
                        return new Promise((resolve, reject) => {
                            return this.service.listBlobsSegmentedWithPrefix(op.container, op.prefix || '', op.token, (error, result) => {
                                if (!error) {
                                    for (const entity of result.entries) {
                                        streams.out.push(entity, op);
                                    }
                                    if (result.continuationToken) {
                                        op.token =
                                            result.continuationToken;
                                        streams.in.buffer.push(op);
                                    }
                                    else {
                                        op.resolve();
                                    }
                                    resolve();
                                }
                                else {
                                    streams.out.emit('error', error);
                                    op.reject(error);
                                    reject(error);
                                }
                            });
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
    stream(operations, inOptions, outOptions) {
        // start the stream
        const streams = this.streams(inOptions || {}, outOptions || {});
        // push the operations
        if (Array.isArray(operations)) {
            for (const operation of operations) {
                streams.in.push(operation);
            }
        }
        else {
            streams.in.push(operations);
        }
        // end the input stream
        streams.in.end();
        return streams.out;
    }
    process(operations, inOptions, outOptions) {
        return new Promise((resolve, reject) => {
            try {
                // start commit
                const stream = this.stream(operations, inOptions, outOptions);
                // resolve when done
                stream.once('end', () => {
                    resolve();
                });
            }
            catch (error) {
                reject(error);
            }
        });
    }
    loadAsStream(container, prefix, outOptions) {
        const loader = this.streams({
            transform: data => new AzureBlobOperation_1.default(container, 'load', data.name)
        }, outOptions || {});
        const listOperation = new AzureBlobOperation_1.default(container, 'list');
        if (prefix) {
            listOperation.type = 'listWithPrefix';
            listOperation.prefix = prefix;
        }
        this.stream(listOperation).pipe(loader.in);
        return loader.out;
    }
    listAsStream(container, prefix, outOptions) {
        const listOperation = new AzureBlobOperation_1.default(container, 'list');
        if (prefix) {
            listOperation.type = 'listWithPrefix';
            listOperation.prefix = prefix;
        }
        return this.stream(listOperation, {}, outOptions);
    }
    // create the container if it doesn't already exist
    createContainerIfNotExists(container) {
        const createContainerIfNotExists = util
            .promisify(azs.BlobService.prototype.createContainerIfNotExists)
            .bind(this.service);
        return createContainerIfNotExists(container);
    }
}
exports.default = AzureBlob;
