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
const es6_promise_pool_1 = __importDefault(require("es6-promise-pool"));
const events_1 = require("events");
class BlobHelper {
    constructor(obj) {
        this.events = new events_1.EventEmitter();
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
        // record the container name
        this.container = obj.container;
    }
    // create or replace an append blob
    createOrReplaceAppendBlob(filename) {
        const createOrReplaceAppendBlob = util.promisify(azs.BlobService.prototype.createOrReplaceAppendBlob).bind(this.service);
        return createOrReplaceAppendBlob(this.container, filename);
    }
    // append content
    appendToBlob(filename, content) {
        const appendBlockFromText = util.promisify(azs.BlobService.prototype.appendBlockFromText).bind(this.service);
        return appendBlockFromText(this.container, filename, content);
    }
    // append content with concurrency
    async appendToBlobs(writes, concurrency = 10) {
        // produce promises to save them
        let index = 0;
        const producer = () => {
            if (index < writes.length) {
                const write = writes[index];
                index++;
                this.events.emit("verbose", `${write.content.length} bytes writing to "${write.filename}"...`);
                return this.appendToBlob(write.filename, write.content).then(_ => {
                    this.events.emit("verbose", `${write.content.length} bytes written to "${write.filename}".`);
                });
            }
            else {
                return undefined;
            }
        };
        // enqueue them x at a time
        const pool = new es6_promise_pool_1.default(producer, concurrency);
        await pool.start();
        // log
        this.events.emit("verbose", `${index} writes(s) committed.`);
    }
    // load a file
    async loadFile(filename, format = "text") {
        const getBlobToText = util.promisify(azs.BlobService.prototype.getBlobToText).bind(this.service);
        const raw = await getBlobToText(this.container, filename);
        switch (format) {
            case "json":
                return JSON.parse(raw);
            case "text":
                return raw;
        }
    }
    // load a set of files
    async loadFiles(filenames, format = "text", concurrency = 10) {
        const set = [];
        // produce promises to load the files
        let index = 0;
        const producer = () => {
            if (index < filenames.length) {
                const filename = filenames[index];
                index++;
                return this.loadFile(filename, format).then(o => {
                    set.push(o);
                });
            }
            else {
                return undefined;
            }
        };
        // load them x at a time
        const pool = new es6_promise_pool_1.default(producer, concurrency);
        await pool.start();
        return set;
    }
    // get a segment of block blobs by prefix
    async listSegmentWithPrefix(prefix, all, token = null) {
        // promify
        const listBlobsSegmentedWithPrefix = util.promisify(azs.BlobService.prototype.listBlobsSegmentedWithPrefix).bind(this.service);
        // recursively keep getting segments
        do {
            this.events.emit("verbose", `listing blobs "${this.container}/${prefix}"...`);
            const result = await listBlobsSegmentedWithPrefix(this.container, prefix, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob")
                    all.push(entry);
            }
            this.events.emit("verbose", `${all.length} block blobs enumerated thusfar...`);
            if (!result.continuationToken)
                break;
            token = result.continuationToken;
        } while (true);
    }
    // list all block blobs in container by prefix
    async listWithPrefix(prefix, pattern) {
        // get all blobs
        const all = [];
        await this.listSegmentWithPrefix(prefix, all);
        this.events.emit("verbose", `${all.length} block blobs found.`);
        // filter if a pattern should be applied
        if (pattern) {
            return all.filter(entry => {
                pattern.lastIndex = 0; // reset
                return pattern.test(entry.name);
            });
        }
        else {
            return all;
        }
    }
    // get a segment of block blobs
    async listSegment(all, token = null) {
        // promify
        const listBlobsSegmented = util.promisify(azs.BlobService.prototype.listBlobsSegmented).bind(this.service);
        // recursively keep getting segments
        do {
            this.events.emit("verbose", `listing blobs "${this.container}"...`);
            const result = await listBlobsSegmented(this.container, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob")
                    all.push(entry);
            }
            this.events.emit("verbose", `${all.length} block blobs enumerated thusfar...`);
            if (!result.continuationToken)
                break;
            token = result.continuationToken;
        } while (true);
    }
    // list all block blobs in container
    async list(pattern) {
        // get all blobs
        const all = [];
        await this.listSegment(all);
        this.events.emit("verbose", `${all.length} block blobs found.`);
        // filter if a pattern should be applied
        if (pattern) {
            return all.filter(entry => {
                pattern.lastIndex = 0; // reset
                return pattern.test(entry.name);
            });
        }
        else {
            return all;
        }
    }
    // create the container if it doesn't already exist
    createContainerIfNotExists() {
        const createContainerIfNotExists = util.promisify(azs.BlobService.prototype.createContainerIfNotExists).bind(this.service);
        return createContainerIfNotExists(this.container);
    }
}
exports.default = BlobHelper;
