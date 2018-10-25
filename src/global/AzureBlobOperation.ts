// includes
import * as azs from 'azure-storage';
import PromiseImposter from './PromiseImposter';

type operationTypes =
    | 'append'
    | 'createAppend'
    | 'load'
    | 'list'
    | 'listWithPrefix'; // lots more need to be added

export default class AzureBlobOperation extends PromiseImposter {
    public container: string;
    public type: operationTypes;
    public filename?: string;
    public content?: string;
    public prefix?: string;
    public token?: azs.common.ContinuationToken;

    /**
     * This class designates a blob operation that can be queued, streamed, etc.
     * After creating an object, you may be alerted when its operation is complete using .then(),
     * .finally(), and trap errors with .catch().
     */
    constructor(
        container: string,
        type: operationTypes,
        filename?: string,
        content?: string
    ) {
        super();
        this.container = container;
        this.filename = filename;
        this.type = type;
        this.content = content;
    }
}
