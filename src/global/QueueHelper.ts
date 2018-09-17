
// includes
import * as azs from "azure-storage";
import * as util from "util";
import PromisePool from "es6-promise-pool";
import { EventEmitter } from "events";

export type encoders = "base64" | "xml" | "binary";

export interface QueueHelperJSON {
    service?:          azs.QueueService
    connectionString?: string,
    account?:          string,
    sas?:              string,
    key?:              string,
    name:              string,
    encoder?:          encoders
}

export default class QueueHelper {

    public events:  EventEmitter = new EventEmitter();
    public service: azs.QueueService;
    public name:    string;

    // returns true if there are any messages in the queue
    public async hasMessages() {
        const getQueueMetadata: (queue: string) => Promise<azs.QueueService.QueueResult> =
            util.promisify(azs.QueueService.prototype.getQueueMetadata).bind(this.service);
        const result = await getQueueMetadata(this.name);
        if (result.approximateMessageCount == null) {
            this.events.emit("verbose", `approximate message count is queue "${this.name}" is "indeterminate".`);
            return true; // it is safer to assume there could be
        } else {
            this.events.emit("verbose", `approximate message count is queue "${this.name}" is "${result.approximateMessageCount}".`);
            return (result.approximateMessageCount > 0);
        }
    }

    // add a single message to the queue
    public enqueueMessage(message: string) {
        const createMessage: (queue: string, message: string) => Promise<azs.QueueService.QueueMessageResult> =
            util.promisify(azs.QueueService.prototype.createMessage).bind(this.service);
        return createMessage(this.name, message);
    }

    // add multiple messages to the queue in parallel
    public async enqueueMessages(messages: string[], concurrency: number = 10) {

        // produce promises to save them
        let index = 0;
        const producer = () => {
            if (index < messages.length) {
                const message = messages[index];
                index++;
                if (index % 100 === 0) this.events.emit("verbose", `${index} message(s) enqueued thusfar...`);
                return this.enqueueMessage(message);
            } else {
                return undefined;
            }
        }
    
        // enqueue them x at a time
        const pool = new PromisePool(producer, concurrency);
        await pool.start();

        // log
        this.events.emit("verbose", `${index} message(s) enqueued.`);

    }

    // create the queue if it doesn't already exist
    public createQueueIfNotExists() {
        const createQueueIfNotExists: (queue: string) => Promise<azs.QueueService.QueueResult> =
            util.promisify(azs.QueueService.prototype.createQueueIfNotExists).bind(this.service);
        return createQueueIfNotExists(this.name);
    }

    // set the encoding method
    public set encoder(option: encoders) {
        switch (option) {
            case "base64":
                this.service.messageEncoder = new azs.QueueMessageEncoder.TextBase64QueueMessageEncoder();
                break;
            case "xml":
                this.service.messageEncoder = new azs.QueueMessageEncoder.TextXmlQueueMessageEncoder();
                break;
            case "binary":
                this.service.messageEncoder = new azs.QueueMessageEncoder.BinaryBase64QueueMessageEncoder();
                break;
        }
    }

    constructor(obj: QueueHelperJSON) {

        // establish the service
        if (obj.service) {
            this.service = obj.service;
        } else if (obj.connectionString) {
            this.service = azs.createQueueService(obj.connectionString);
            if (obj.encoder) this.encoder = obj.encoder;
        } else if (obj.account && obj.sas) {
            const host = `https://${obj.account}.queue.core.windows.net`;
            this.service = azs.createQueueServiceWithSas(host, obj.sas);
            if (obj.encoder) this.encoder = obj.encoder;
        } else if (obj.account && obj.key) {
            this.service = azs.createQueueService(obj.account, obj.key);
            if (obj.encoder) this.encoder = obj.encoder;
        } else {
            throw new Error(`You must specify service, connectionString, account/sas, or account/key.`)
        }

        // record the queue name
        this.name = obj.name;

    }

}