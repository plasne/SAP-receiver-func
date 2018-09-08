
// FUNCTION: receiver
// AUTHOR:   Peter Lasne, Principal Software Development Engineer
// PURPOSE:  This function accepts IDOC messages from SAP and writes them to Blob Storage.

// includes
const uuidv4 = require("uuid/v4");
const agentKeepAlive = require("agentkeepalive");
const querystring = require("query-string");
const crypto = require("crypto");
const request = require("request");
const moment = require("moment");
require("moment-round");

// prototype extensions
String.prototype.replaceAll = function(search, replacement) {
    var target = this;
    return target.replace(new RegExp(search, "g"), replacement);
};

// globals
const ACCOUNT    = process.env.STORAGE_ACCOUNT;
const CONTAINER  = process.env.STORAGE_CONTAINER_INPUT;
const SAS        = process.env.STORAGE_SAS;
const KEY        = process.env.STORAGE_KEY;
const PERIOD     = process.env.FOLDER_PERIOD       || "1 hour";
const FORMAT     = process.env.FOLDER_FORMAT       || "YYYYMMDDTHHmmss";

// use an HTTP(s) agent with keepalive and connection pooling
const agent = new agentKeepAlive.HttpsAgent({
    maxSockets: 40,
    maxFreeSockets: 10,
    timeout: 60000,
    freeSocketKeepAliveTimeout: 30000
});

function generateSignature(context, path, options) {

    // pull out all querystring parameters so they can be sorted and used in the signature
    const parameters = [];
    const parsed = querystring.parseUrl(options.url);
    for (const key in parsed.query) {
        parameters.push(`${key}:${parsed.query[key]}`);
    }
    parameters.sort((a, b) => a.localeCompare(b));

    // pull out all x-ms- headers so they can be sorted and used in the signature
    const xheaders = [];
    for (const key in options.headers) {
        if (key.substring(0, 5) === "x-ms-") {
            xheaders.push(`${key}:${options.headers[key]}`);
        }
    }
    xheaders.sort((a, b) => a.localeCompare(b));

    // zero length for the body is an empty string, not 0
    const len = (options.body) ? Buffer.byteLength(options.body) : "";

    // potential content-type, if-none-match
    const ct = options.headers["Content-Type"] || "";
    const none = options.headers["If-None-Match"] || "";

    // generate the signature line
    let raw = `${options.method}\n\n\n${len}\n\n${ct}\n\n\n\n${none}\n\n\n${xheaders.join("\n")}\n/${ACCOUNT}/${CONTAINER}`;
    if (path) raw += `/${path}`;
    raw += (parameters.length > 0) ? `\n${parameters.join("\n")}` : "";
    context.log.verbose(`The unencoded signature is "${raw.replaceAll("\n", "\\n")}"`);

    // sign it
    const hmac = crypto.createHmac("sha256", new Buffer.from(KEY, "base64"));
    const signature = hmac.update(raw, "utf-8").digest("base64");
    
    // return the Authorization header
    return `SharedKey ${ACCOUNT}:${signature}`;

}

async function createBlockBlob(context, filename, body, metadata) {
    try {
        context.log.verbose(`creating block blob "${filename}"...`);

        // specify the request options, including the headers
        const options = {
            method: "PUT",
            agent: agent,
            url: `https://${ACCOUNT}.blob.core.windows.net/${CONTAINER}/${filename}${SAS || ""}`,
            headers: {
                "x-ms-version": "2017-07-29",
                "x-ms-date": (new Date()).toUTCString(),
                "x-ms-blob-type": "BlockBlob",
                "Content-Type": "application/xml"
            },
            body: body
        };

        // add metadata
        if (metadata) {
            for (const name in metadata) {
                options.headers[`x-ms-meta-${name}`] = metadata[name];
            }
        }

        // generate and apply the signature
        if (!SAS) {
            const signature = generateSignature(context, filename, options);
            options.headers.Authorization = signature;
        }
        
        // commit
        await new Promise((resolve, reject) => {
            request(options, (error, response, body) => {
                if (!error && response.statusCode >= 200 && response.statusCode < 300) {
                    context.log.verbose(`created block blob "${filename}".`);
                    resolve(body);
                } else if (error) {
                    reject(error);
                } else {
                    reject(new Error(`HTTP response ${response.statusCode}: ${response.statusMessage}`));
                }
            });
        });

    } catch (error) {
        context.log.error(`failed to create block blob "${filename}"`);
        throw error;
    }
}

module.exports = async (context, req) => {

    // validate
    if (ACCOUNT && CONTAINER && (SAS || KEY) && PERIOD && FORMAT) {
        // requirements are met
    } else {
        context.log.error("You must specify ACCOUNT, CONTAINER, and SAS or KEY.");
        context.res.status = 500;
        return;
    }

    // generate a coorelation ID for logging
    const coorelationId = uuidv4();
    context.log.verbose(`POST received...`, { coorelationId: coorelationId });
    try {
        const promises = [];

        // determine the timeslice to apply the files to
        const now = new moment();
        const period_array = PERIOD.split(" ");
        const period_last = now.floor(Number.parseInt(period_array[0]), period_array[1]);
        const period_path = period_last.utc().format(FORMAT);
        
        // promise to save the raw file
        const raw = createBlockBlob(context, `${period_path}/name-${uuidv4()}.xml`, req.rawBody, { processed: false }, { coorelationId: coorelationId });
        promises.push(raw);

        // respond when all promises are fulfilled
        await Promise.all(promises);
        context.log.verbose(`POST response is 200.`, { coorelationId: coorelationId });
        context.res.status = 200;

    } catch (error) {

        // handle any exceptions
        context.log.verbose("debug", `POST response is 500.`, { coorelationId: coorelationId });
        context.res.status = 500;
        context.log.error(error.stack);

    }

}