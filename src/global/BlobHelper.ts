
// includes
import * as azs from "azure-storage";
import * as util from "util";
import PromisePool from "es6-promise-pool";
import { EventEmitter } from "events";

export type formats = "json" | "text";

export interface BlobHelperJSON {
    service?:          azs.BlobService
    connectionString?: string,
    account?:          string,
    sas?:              string,
    key?:              string,
    container:         string
}

export interface BlobHelperBatchWrite {
    filename: string,
    content:  string
}

export default class BlobHelper {

    public events:    EventEmitter = new EventEmitter();
    public service:   azs.BlobService;
    public container: string;

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

    // append content with concurrency
    public async appendToBlobs(writes: BlobHelperBatchWrite[], concurrency = 10) {

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
            } else {
                return undefined;
            }
        }
    
        // enqueue them x at a time
        const pool = new PromisePool(producer, concurrency);
        await pool.start();

        // log
        this.events.emit("verbose", `${index} writes(s) committed.`);

    }

    // load a file
    public async loadFile(filename: string, format: formats = "text") {
        const getBlobToText: (container: string, blob: string) => Promise<string> =
            util.promisify(azs.BlobService.prototype.getBlobToText).bind(this.service);
        const raw = await getBlobToText(this.container, filename);
        switch (format) {
            case "json":
                return JSON.parse(raw);
            case "text":
                return raw;
        }
    }

    // load a set of files
    public async loadFiles(filenames: string[], format: formats = "text", concurrency: number = 10) {
        const set: any[] = [];

        // produce promises to load the files
        let index = 0;
        const producer = () => {
            if (index < filenames.length) {
                const filename = filenames[index];
                index++;
                return this.loadFile(filename, format).then(o => {
                    set.push(o);
                });
            } else {
                return undefined;
            }
        }
    
        // load them x at a time
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
            this.events.emit("verbose", `listing blobs "${this.container}/${prefix}"...`);
            const result = await listBlobsSegmentedWithPrefix(this.container, prefix, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob") all.push(entry);
            }
            this.events.emit("verbose", `${all.length} block blobs enumerated thusfar...`);
            if (!result.continuationToken) break;
            token = result.continuationToken;
        } while (true);

    }

    // list all block blobs in container by prefix
    public async listWithPrefix(prefix: string, pattern?: RegExp) {

        // get all blobs
        const all: azs.BlobService.BlobResult[] = [];
        await this.listSegmentWithPrefix(prefix, all);
        this.events.emit("verbose", `${all.length} block blobs found.`);

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
            this.events.emit("verbose", `listing blobs "${this.container}"...`);
            const result = await listBlobsSegmented(this.container, token);
            for (const entry of result.entries) {
                if (entry.blobType === "BlockBlob") all.push(entry);
            }
            this.events.emit("verbose", `${all.length} block blobs enumerated thusfar...`);
            if (!result.continuationToken) break;
            token = result.continuationToken;
        } while (true);

    }

    // list all block blobs in container
    public async list(pattern?: RegExp) {

        // get all blobs
        const all: azs.BlobService.BlobResult[] = [];
        await this.listSegment(all);
        this.events.emit("verbose", `${all.length} block blobs found.`);

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

    constructor(obj: BlobHelperJSON) {

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
            throw new Error(`You must specify service, connectionString, account/sas, or account/key.`)
        }

        // record the container name
        this.container = obj.container;

    }

}