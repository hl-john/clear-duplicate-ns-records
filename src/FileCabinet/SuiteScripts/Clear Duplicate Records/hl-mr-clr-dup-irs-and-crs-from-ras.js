/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */
define(["N/record", "N/runtime", "N/search"], /**
 * @param{record} record
 * @param{runtime} runtime
 * @param{search} search
 */ (record, runtime, search) => {
    const readDuplicateRecords = (internalIds) => {
        const internalIdCol = { name: "internalid" };
        const tranidCol = { name: "tranid", sort: search.Sort.ASC };
        const typeCol = { name: "type" };
        const recordTypeCol = { name: "recordtype" };
        const trandateCol = { name: "trandate" };
        const dateCreatedCol = { name: "datecreated" };
        const createdFromCol = { name: "createdfrom" };
        const createdByCol = { name: "createdby" };

        const txnSearch = search.create({
            type: "transaction",
            filters: [["createdfrom.internalid", "anyof", internalIds], "AND", ["createdfrom.type", "anyof", "RtnAuth"], "AND", ["mainline", "is", "T"]],
            columns: [internalIdCol, tranidCol, typeCol, recordTypeCol, trandateCol, dateCreatedCol, createdFromCol, createdByCol],
        });

        const results = [];

        const pagedData = txnSearch.runPaged({ pageSize: 1000 });
        for (let i = 0; i < pagedData.pageRanges.length; i++) {
            const searchPage = pagedData.fetch({ index: i });

            searchPage.data.forEach((result) => {
                const id = result.getValue(internalIdCol);
                const tranid = result.getValue(tranidCol);
                const type = result.getValue(typeCol);
                const recordType = result.getValue(recordTypeCol);
                const trandate = result.getValue(trandateCol);
                const dateCreated = result.getValue(dateCreatedCol);
                const createdFrom = result.getValue(createdFromCol);
                const createdBy = result.getValue(createdByCol);

                results.push({ id, tranid, type, recordType, trandate, dateCreated, createdFrom, createdBy });
            });
        }

        return results;
    };

    /**
     * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
     * @param {Object} inputContext
     * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {Object} inputContext.ObjectRef - Object that references the input data
     * @typedef {Object} ObjectRef
     * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
     * @property {string} ObjectRef.type - Type of the record instance that contains the input data
     * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
     * @since 2015.2
     */

    const getInputData = (inputContext) => {
        try {
            const script = runtime.getCurrentScript();
            const raIds = script.getParameter({ name: "custscript_ras_with_duplicate_children" });

            const raIdsArray = raIds
                .split(/\n/)
                .filter((id) => id.trim() !== "")
                .map((id) => id.trim());
            const uniqueRaIds = [...new Set(raIdsArray)];
            log.debug(`getInputData: reading duplicate records for ${uniqueRaIds.length} RAs`, uniqueRaIds);

            const duplicateRecords = readDuplicateRecords(uniqueRaIds);
            log.debug(`getInputData: ${duplicateRecords.length} duplicate records`, duplicateRecords);

            return duplicateRecords;
        } catch (error) {
            log.error("getInputDataError: reading duplicate records", error);
            return [];
        }
    };

    /**
     * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
     * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
     * context.
     * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
     *     is provided automatically based on the results of the getInputData stage.
     * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
     *     function on the current key-value pair
     * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
     *     pair
     * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {string} mapContext.key - Key to be processed during the map stage
     * @param {string} mapContext.value - Value to be processed during the map stage
     * @since 2015.2
     */

    const map = (mapContext) => {
        try {
            const recordInfo = JSON.parse(mapContext.value);
            mapContext.write({ key: recordInfo.createdFrom, value: mapContext.value });
        } catch (error) {
            log.error("mapError: processing duplicate records", error);
        }
    };

    /**
     * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
     * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
     * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
     *     provided automatically based on the results of the map stage.
     * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
     *     reduce function on the current group
     * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
     * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {string} reduceContext.key - Key to be processed during the reduce stage
     * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
     *     for processing
     * @since 2015.2
     */
    const reduce = (reduceContext) => {
        const createdFrom = reduceContext.key;
        const txnRecords = reduceContext.values;

        try {
            log.debug(`reduce: processing duplicate records for RA ${createdFrom}`, txnRecords);

            let irsCount = 0;
            let crsCount = 0;

            for (const txnRecord of txnRecords) {
                const { id, recordType } = JSON.parse(txnRecord);

                if (recordType === "itemreceipt") irsCount++;
                if (recordType === "cashrefund") crsCount++;

                if (irsCount > 1 || crsCount > 1) {
                    const recName = recordType === "itemreceipt" ? "IRS" : recordType === "cashrefund" ? "CRS" : "<NoneRecName>";
                    log.debug(`reduce: deleting duplicate ${recName}`, txnRecord);

                    const recordId = `${createdFrom}-${recordType}-${id}`;
                    try {
                        record.delete({ type: recordType, id: id });
                        if (recName === "IRS") irsCount--;
                        if (recName === "CRS") crsCount--;

                        reduceContext.write({ key: "success-delete", value: recordId });
                        log.debug(`reduce: deleted duplicate ${recName} with ID ${recordId}`);
                    } catch (error) {
                        reduceContext.write({ key: "error-delete", value: recordId });
                        log.error(`reduce: error deleting duplicate ${recName} with ID ${recordId}`, error);
                    }
                }
            }
        } catch (error) {
            log.error(`reduceError: processing duplicate records for RA ${createdFrom}`, error);
        }
    };

    /**
     * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
     * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
     * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
     * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
     *     script
     * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
     * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
     *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
     * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
     * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
     * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
     *     script
     * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
     * @param {Object} summaryContext.inputSummary - Statistics about the input stage
     * @param {Object} summaryContext.mapSummary - Statistics about the map stage
     * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
     * @since 2015.2
     */
    const summarize = (summaryContext) => {
        try {
            const successes = [];
            const errors = [];

            summaryContext.output.iterator().each(function (key, value) {
                if (key === "success-delete") successes.push(value);
                if (key === "error-delete") errors.push(value);
                return true;
            });

            log.debug(`${successes.length} successes`, successes);
            log.debug(`${errors.length} errors`, errors);
        } catch (error) {
            log.error("Unable to summarize", error);
        }
    };

    return { getInputData, map, reduce, summarize };
});
