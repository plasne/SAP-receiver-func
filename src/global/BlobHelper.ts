
// includes
import { Context } from "azure-functions-ts-essentials";
import * as azs from "azure-storage";
import * as util from "util";
import PromisePool from "es6-promise-pool";

export default class BlobHelper {

    private context:   Context;
    private service:   azs.BlobService;
    private container: string;

    // create or replace an append blob
    public createOrReplaceAppendBlob(filename: string) {
        const createOrReplaceAppendBlob: (container: string, blob: string) => Promise<void> =
            util.promisify(azs.BlobService.prototype.createOrReplaceAppendBlob).bind(this.service);
        return createOrReplaceAppendBlob(this.container, filename);
    }

    // append content
    public appendToBlob(filename: string, content: string) {
        const appendBlockFromText: (container: string, blob: string, content: string) => Promise<azs.BlobService.BlobResult> =
            util.promisify(azs.BlobService.prototype.appendBlockFromText).bind(this.service);
        return appendBlockFromText(this.container, filename, content);
    }

    // load a file
    public loadFile(filename: string) {
        const getBlobToText: (container: string, blob: string) => Promise<string> =
            util.promisify(azs.BlobService.prototype.getBlobToText).bind(this.service);
        return getBlobToText(this.container, filename);
    }

    // load a set of files
    public async loadFiles(filenames: string[], concurrency: number = 10) {
        const set: any[] = [];

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
            } else {
                return undefined;
            }
        }
    
        // load them 10 at a time
        const pool = new PromisePool(producer, concurrency);
        await pool.start();

        return set;
    }

    // get a segment of block blobs by prefix
    private async listSegmentWithPrefix(prefix: string, all: azs.BlobService.BlobResult[], token: azs.common.ContinuationToken | null = null) {

        // promify
        const listBlobsSegmentedWithPrefix: (container: string, prefix: string, token: azs.common.ContinuationToken | null) =>
            Promise<azs.BlobService.ListBlobsResult> = util.promisify(azs.BlobService.prototype.listBlobsSegmentedWithPrefix).bind(this.service);

        // recursively keep getting segments
        do {
            if (this.context.log) this.context.log.verbose(`listing blobs "${this.container}/${prefix}"...`);
            const result = await listBlobsSegmentedWithPrefix(this.container, prefix, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob") all.push(entry);
            }
            if (this.context.log) this.context.log.verbose(`${all.length} block blobs enumerated thusfar...`);
            if (!result.continuationToken) break;
            token = result.continuationToken;
        } while (true);

    }

    // list all block blobs in container by prefix
    public async listWithPrefix(prefix: string, pattern?: RegExp) {

        // get all blobs
        const all: azs.BlobService.BlobResult[] = [];
        await this.listSegmentWithPrefix(prefix, all);
        if (this.context.log) this.context.log.verbose(`${all.length} block blobs found.`);

        // filter if a pattern should be applied
        if (pattern) {
            return all.filter(entry => {
                pattern.lastIndex = 0; // reset
                return pattern.test(entry.name);
            });
        } else {
            return all;
        }

    }

    // get a segment of block blobs
    private async listSegment(all: azs.BlobService.BlobResult[], token: azs.common.ContinuationToken | null = null) {

        // promify
        const listBlobsSegmented: (container: string, token: azs.common.ContinuationToken | null) =>
            Promise<azs.BlobService.ListBlobsResult> = util.promisify(azs.BlobService.prototype.listBlobsSegmented).bind(this.service);

        // recursively keep getting segments
        do {
            if (this.context.log) this.context.log.verbose(`listing blobs "${this.container}"...`);
            const result = await listBlobsSegmented(this.container, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob") all.push(entry);
            }
            if (this.context.log) this.context.log.verbose(`${all.length} block blobs enumerated thusfar...`);
            if (!result.continuationToken) break;
            token = result.continuationToken;
        } while (true);

    }

    // list all block blobs in container
    public async list(pattern?: RegExp) {

        // get all blobs
        const all: azs.BlobService.BlobResult[] = [];
        await this.listSegment(all);
        if (this.context.log) this.context.log.verbose(`${all.length} block blobs found.`);

        // filter if a pattern should be applied
        if (pattern) {
            return all.filter(entry => {
                pattern.lastIndex = 0; // reset
                return pattern.test(entry.name);
            });
        } else {
            return all;
        }

    }

    // create the container if it doesn't already exist
    public createContainerIfNotExists() {
        const createContainerIfNotExists: (container: string) => Promise<azs.BlobService.ContainerResult> =
            util.promisify(azs.BlobService.prototype.createContainerIfNotExists).bind(this.service);
        return createContainerIfNotExists(this.container);
    }

    constructor(context: Context, service: azs.BlobService, container: string) {
        this.context = context;
        this.service = service;
        this.container = container;
    }

}