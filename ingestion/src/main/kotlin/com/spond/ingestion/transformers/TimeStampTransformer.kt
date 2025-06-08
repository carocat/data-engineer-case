package com.spond.ingestion.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.try_to_timestamp
import org.apache.spark.sql.functions.col

/**
 * Transformer for parsing timestamp columns safely using a fixed ISO 8601 pattern.
 *
 * This transformer uses Spark's `try_to_timestamp` to convert a string column into a TimestampType column.
 * It preserves the original column name or writes to a specified output column.
 *
 * If the input value is non-null but fails to parse, the row is considered malformed and will be printed to stdout.
 *
 * @param inputCol Name of the column to parse as a timestamp (must be a StringType column).
 * @param outputCol Name of the resulting column after conversion. Defaults to overwriting the input column.
 * @param uid Unique identifier for the transformer.
 */
class TimeStampTransformer(
    private val inputCol: String,
    private val outputCol: String = inputCol,
    uid: String = Identifiable.randomUID("TimeStampTransformer")
) : BaseTransformer(uid) {

    /**
     * Applies the timestamp transformation to the input dataset.
     *
     * - Converts [inputCol] to [outputCol] using the pattern `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`.
     * - Logs malformed rows where parsing fails despite non-null input.
     *
     * @param dataset Input Dataset of Rows containing the timestamp column.
     * @return Dataset with the [outputCol] added or overwritten as TimestampType.
     */
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        val transformed = dataset.withColumn(
            outputCol,
            try_to_timestamp(col(inputCol))
        )

        val malformed = transformed
            .filter(col(inputCol).isNotNull.and(col(outputCol).isNull))


        if (!malformed.isEmpty) {
            println("⚠️ Malformed timestamp rows in column `$inputCol`:")
            malformed.show(false)
        }

        return transformed
    }
}
