
// PROCESSES PRIORITY MESSAGES, LIKE CREATING THE FILES

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