"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const PromiseImposter_1 = __importDefault(require("./PromiseImposter"));
class AzureBlobOperation extends PromiseImposter_1.default {
    /**
     * This class designates a blob operation that can be queued, streamed, etc.
     * After creating an object, you may be alerted when its operation is complete using .then(),
     * .finally(), and trap errors with .catch().
     */
    constructor(container, type, filename, content) {
        super();
        this.container = container;
        this.filename = filename;
        this.type = type;
        this.content = content;
    }
}
exports.default = AzureBlobOperation;
