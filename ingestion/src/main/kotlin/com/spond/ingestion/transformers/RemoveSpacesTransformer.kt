package com.spond.ingestion.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions

/**
 * Transformer that removes leading and trailing spaces from a specified string column.
 *
 * @param inputCol The name of the input column to clean.
 * @param outputCol The name of the output column to store the trimmed values. Defaults to [inputCol].
 * @param uid Unique identifier for the transformer.
 */
class RemoveSpacesTransformer(
    private val inputCol: String,
    private val outputCol: String = inputCol,
    uid: String = Identifiable.randomUID("RemoveSpacesTransformer")
) : BaseTransformer(uid) {

    /**
     * Applies trim to remove leading and trailing whitespace from the [inputCol].
     * The result is stored in [outputCol].
     *
     * @param dataset The input dataset.
     * @return A new dataset with trimmed string values.
     */
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        return dataset.withColumn(outputCol, functions.trim(functions.col(inputCol)))
    }
}
