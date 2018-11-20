// FUNCTION: receiver()
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function accepts IDOC messages from SAP and writes them to Blob Storage.

// includes
import { Context, HttpStatusCode } from 'azure-functions-ts-essentials';
import moment = require('moment');
require('moment-round');
import * as http from 'http';
import * as https from 'https';
import { v4 as uuid } from 'uuid';
import AzureBlob from '../global/AzureBlob';

// variables
const STORAGE_ACCOUNT: string | undefined = process.env.STORAGE_ACCOUNT;
const STORAGE_CONTAINER_INPUT: string | undefined =
    process.env.STORAGE_CONTAINER_INPUT;
const STORAGE_SAS: string | undefined = process.env.STORAGE_SAS;
const STORAGE_KEY: string | undefined = process.env.STORAGE_KEY;
const FOLDER_PERIOD: string = process.env.FOLDER_PERIOD || '1 hour';
const FOLDER_FORMAT: string = process.env.FOLDER_FORMAT || 'YYYYMMDDTHHmmss';

// modify the agents
const httpAgent: any = http.globalAgent;
httpAgent.keepAlive = true;
httpAgent.maxSockets = 30;
const httpsAgent: any = https.globalAgent;
httpsAgent.keepAlive = true;
httpsAgent.maxSockets = 30;

export async function run(context: Context) {
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
        if (context.log) context.log.verbose('validated succesfully');

        // establish connections
        const blob = new AzureBlob({
            account: STORAGE_ACCOUNT,
            key: STORAGE_KEY,
            sas: STORAGE_SAS,
            useGlobalAgent: true
        });

        // determine the timeslice to apply the files to
        const now = moment();
        const periodArray = FOLDER_PERIOD.split(' ');
        const periodLast = now.floor(
            Number.parseInt(periodArray[0], 10),
            periodArray[1]
        );
        const periodPath = periodLast.utc().format(FOLDER_FORMAT);

        // save the raw file
        if (context.req.rawBody) {
            if (context.log) {
                context.log.info(
                    `saving "${STORAGE_CONTAINER_INPUT}/${periodPath}/name-${uuid()}.xml"...`
                );
            }
            await blob.createBlockBlobFromText(
                STORAGE_CONTAINER_INPUT,
                `${periodPath}/name-${uuid()}.xml`,
                context.req.rawBody
            );
            if (context.log) {
                context.log.info(
                    `saved "${STORAGE_CONTAINER_INPUT}/${periodPath}/name-${uuid()}.xml".`
                );
            }
        } else {
            if (context.log) context.log.info(`ignoring request without body.`);
        }

        // respond with status
        context.res.status = HttpStatusCode.OK;
        context.res.body = { status: 'success' };
    } catch (error) {
        if (context.res) {
            context.res.status = HttpStatusCode.InternalServerError;
        }
        if (context.log) context.log.error(error.stack);
    }
}
