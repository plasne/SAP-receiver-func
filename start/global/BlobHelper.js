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
const azs = __importStar(require("azure-storage"));
const util = __importStar(require("util"));
const es6_promise_pool_1 = __importDefault(require("es6-promise-pool"));
class BlobHelper {
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
    // load a file
    loadFile(filename) {
        const getBlobToText = util.promisify(azs.BlobService.prototype.getBlobToText).bind(this.service);
        return getBlobToText(this.container, filename);
    }
    // load a set of files
    async loadFiles(filenames, concurrency = 10) {
        const set = [];
        // produce promises to load the files
        let index = 0;
        const producer = () => {
            if (index < filenames.length) {
                const filename = filenames[index];
                index++;
                return this.loadFile(filename).then(raw => {
                    const obj = JSON.parse(raw);
                    set.push(obj);
                });
            }
            else {
                return undefined;
            }
        };
        // load them 10 at a time
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
            if (this.context.log)
                this.context.log.verbose(`listing blobs "${this.container}/${prefix}"...`);
            const result = await listBlobsSegmentedWithPrefix(this.container, prefix, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob")
                    all.push(entry);
            }
            if (this.context.log)
                this.context.log.verbose(`${all.length} block blobs enumerated thusfar...`);
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
        if (this.context.log)
            this.context.log.verbose(`${all.length} block blobs found.`);
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
            if (this.context.log)
                this.context.log.verbose(`listing blobs "${this.container}"...`);
            const result = await listBlobsSegmented(this.container, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob")
                    all.push(entry);
            }
            if (this.context.log)
                this.context.log.verbose(`${all.length} block blobs enumerated thusfar...`);
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
        if (this.context.log)
            this.context.log.verbose(`${all.length} block blobs found.`);
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
    constructor(context, service, container) {
        this.context = context;
        this.service = service;
        this.container = container;
    }
}
exports.default = BlobHelper;
