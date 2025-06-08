package com.spond.ingestion.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

/**
 * Transformer that removes rows with null values in a specified column.
 *
 * @param inputCol The name of the column to check for nulls.
 * @param uid Unique identifier for the transformer.
 */
class RemoveNullsTransformer(
    private val inputCol: String,
    uid: String = Identifiable.randomUID("RemoveNullsTransformer")
) : BaseTransformer(uid) {

    /**
     * Filters out rows where [inputCol] is null.
     *
     * @param dataset Input dataset to transform.
     * @return A dataset with rows containing null in [inputCol] removed.
     */
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        return dataset.filter(col(inputCol).isNotNull)
    }
}
