package com.spond.ingestion.util

import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory

object DatasetLogger {
    private val logger = LoggerFactory.getLogger("DatasetLogger")

    /**
     * Logs the schema and shows a sample of the dataset.
     *
     * @param ds The Dataset to inspect.
     * @param label Descriptive label to identify the dataset in logs.
     */
    fun <T> logAndShow(ds: Dataset<T>, label: String) {
        logger.info("\n=== {} ===\n\uD83D\uDCCASchema:\n{}", label, ds.schema().treeString())
        ds.show(false)
    }
}
