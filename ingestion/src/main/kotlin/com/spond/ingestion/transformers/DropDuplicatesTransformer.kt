package com.spond.ingestion.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Transformer that removes duplicate rows from a dataset using all columns.
 *
 * @param uid Unique identifier for the transformer.
 */
class DropDuplicatesTransformer(
    uid: String = Identifiable.randomUID("DropDuplicatesTransformer")
) : BaseTransformer(uid) {

    /**
     * Applies `dropDuplicates()` to remove duplicate rows from the dataset.
     */
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        return dataset.dropDuplicates()
    }
}
