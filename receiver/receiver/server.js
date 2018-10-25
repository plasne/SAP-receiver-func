"use strict";
// FUNCTION: receiver()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function accepts IDOC messages from SAP and writes them to Blob Storage.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const azure_functions_ts_essentials_1 = require("azure-functions-ts-essentials");
const moment = require("moment");
require('moment-round');
const uuid_1 = require("uuid");
const AzureBlob_1 = __importDefault(require("../global/AzureBlob"));
// variables
const STORAGE_ACCOUNT = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT = process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_SAS = process.env.STORAGE_SAS;
const STORAGE_KEY = process.env.STORAGE_KEY;
const FOLDER_PERIOD = process.env.FOLDER_PERIOD || '1 hour';
const FOLDER_FORMAT = process.env.FOLDER_FORMAT || 'YYYYMMDDTHHmmss';
async function run(context) {
    try {
        // validate
        if (!STORAGE_ACCOUNT) {
            throw new Error('STORAGE_ACCOUNT is not defined.');
        }
        if (!STORAGE_CONTAINER_INPUT) {
            throw new Error('STORAGE_CONTAINER_INPUT is not defined.');
        }
        if (!STORAGE_SAS && !STORAGE_KEY) {
            throw new Error('STORAGE_SAS or STORAGE_KEY must be defined.');
        }
        if (!STORAGE_CONTAINER_INPUT) {
            throw new Error('STORAGE_CONTAINER_INPUT is not defined.');
        }
        if (!context.req || !context.res) {
            throw new Error('Request/Response must be defined in bindings.');
        }
        if (context.log)
            context.log.verbose('validated succesfully');
        // establish connections
        const blob = new AzureBlob_1.default({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS
        });
        // determine the timeslice to apply the files to
        const now = moment();
        const periodArray = FOLDER_PERIOD.split(' ');
        const periodLast = now.floor(Number.parseInt(periodArray[0], 10), periodArray[1]);
        const periodPath = periodLast.utc().format(FOLDER_FORMAT);
        // save the raw file
        if (context.log) {
            context.log.info(`saving "${STORAGE_CONTAINER_INPUT}/${periodPath}/name-${uuid_1.v4()}.xml"...`);
        }
        await blob.createBlockBlobFromText(STORAGE_CONTAINER_INPUT, `${periodPath}/name-${uuid_1.v4()}.xml`, context.req.rawBody);
        if (context.log) {
            context.log.info(`saved "${STORAGE_CONTAINER_INPUT}/${periodPath}/name-${uuid_1.v4()}.xml".`);
        }
        // respond with status
        context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.OK;
        context.res.body = { status: 'success' };
    }
    catch (error) {
        if (context.res) {
            context.res.status = azure_functions_ts_essentials_1.HttpStatusCode.InternalServerError;
        }
        if (context.log)
            context.log.error(error.stack);
    }
}
exports.run = run;
