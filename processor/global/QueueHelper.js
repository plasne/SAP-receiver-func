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
class QueueHelper {
    constructor(obj) {
        this.events = new events_1.EventEmitter();
        // establish the service
        if (obj.service) {
            this.service = obj.service;
        }
        else if (obj.connectionString) {
            this.service = azs.createQueueService(obj.connectionString);
            if (obj.encoder)
                this.encoder = obj.encoder;
        }
        else if (obj.account && obj.sas) {
            const host = `https://${obj.account}.queue.core.windows.net`;
            this.service = azs.createQueueServiceWithSas(host, obj.sas);
            if (obj.encoder)
                this.encoder = obj.encoder;
        }
        else if (obj.account && obj.key) {
            this.service = azs.createQueueService(obj.account, obj.key);
            if (obj.encoder)
                this.encoder = obj.encoder;
        }
        else {
            throw new Error(`You must specify service, connectionString, account/sas, or account/key.`);
        }
        // record the queue name
        this.name = obj.name;
    }
    // returns true if there are any messages in the queue
    async hasMessages() {
        const getQueueMetadata = util.promisify(azs.QueueService.prototype.getQueueMetadata).bind(this.service);
        const result = await getQueueMetadata(this.name);
        if (result.approximateMessageCount == null) {
            this.events.emit("verbose", `approximate message count is queue "${this.name}" is "indeterminate".`);
            return true; // it is safer to assume there could be
        }
        else {
            this.events.emit("verbose", `approximate message count is queue "${this.name}" is "${result.approximateMessageCount}".`);
            return (result.approximateMessageCount > 0);
        }
    }
    // add a single message to the queue
    enqueueMessage(message) {
        const createMessage = util.promisify(azs.QueueService.prototype.createMessage).bind(this.service);
        return createMessage(this.name, message);
    }
    // add multiple messages to the queue in parallel
    async enqueueMessages(messages, concurrency = 10) {
        // produce promises to save them
        let index = 0;
        const producer = () => {
            if (index < messages.length) {
                const message = messages[index];
                index++;
                if (index % 100 === 0)
                    this.events.emit("verbose", `${index} message(s) enqueued thusfar...`);
                return this.enqueueMessage(message);
            }
            else {
                return undefined;
            }
        };
        // enqueue them x at a time
        const pool = new es6_promise_pool_1.default(producer, concurrency);
        await pool.start();
        // log
        this.events.emit("verbose", `${index} message(s) enqueued.`);
    }
    // create the queue if it doesn't already exist
    createQueueIfNotExists() {
        const createQueueIfNotExists = util.promisify(azs.QueueService.prototype.createQueueIfNotExists).bind(this.service);
        return createQueueIfNotExists(this.name);
    }
    // set the encoding method
    set encoder(option) {
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
}
exports.default = QueueHelper;
