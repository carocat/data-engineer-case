package com.spond.common.transformers

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions

/**
 * A custom Spark transformer that extracts date partition columns
 * (`year`, `month`, `day`) from a given timestamp column.
 *
 * This is useful for preparing datasets for time-based partitioning
 * when writing to formats like Parquet or Delta Lake.
 *
 * Example:
 * ```
 * val transformer = AddDatePartitionsTransformer("createdAt")
 * val transformed = transformer.transform(df)
 * ```
 *
 * @property inputCol Name of the timestamp column to extract partition values from.
 * @property yearCol Name of the output column containing the extracted year. Default is "year".
 * @property monthCol Name of the output column containing the extracted month. Default is "month".
 * @property dayCol Name of the output column containing the extracted day. Default is "day".
 * @constructor Creates a new transformer instance with a unique identifier.
 */
class AddDatePartitionsTransformer(
    private val inputCol: String,
    private val yearCol: String = "year",
    private val monthCol: String = "month",
    private val dayCol: String = "day",
    uid: String = Identifiable.randomUID("AddDatePartitionsTransformer")
) : BaseTransformer(uid) {

    /**
     * Adds year, month, and day columns to the input dataset
     * based on the specified timestamp column.
     *
     * @param dataset The input dataset.
     * @return A new dataset with the additional date partition columns.
     */
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        return dataset
            .withColumn(yearCol, functions.year(functions.col(inputCol)))
            .withColumn(monthCol, functions.month(functions.col(inputCol)))
            .withColumn(dayCol, functions.dayofmonth(functions.col(inputCol)))
    }
}
