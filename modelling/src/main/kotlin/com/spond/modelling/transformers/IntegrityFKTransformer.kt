package com.spond.modelling.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.broadcast

/**
 * A generic transformer that enforces foreign key constraints by performing an inner join
 * between a child dataset and its parent dataset based on specified key columns.
 *
 * @param T The type of the parent dataset rows (e.g., Team, Membership)
 */
class IntegrityFKTransformer<T>(
    private val parent: Dataset<T>,
    private val parentKeyCol: String,
    private val childKeyCol: String,
    uid: String = Identifiable.randomUID("IntegrityFKTransformer")
) : BaseTransformer(uid) {

    // Enforce referential integrity by joining with the parent dataset on the foreign key.
    // Use broadcast join for performance when the parent dataset is small to improve performance.
    // Drop the parent key column after the join to avoid duplication and name conflicts.
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        val parentCol = parent.col(parentKeyCol)
        val childCol = dataset.col(childKeyCol)

        return dataset
            .join(broadcast(parent), childCol.equalTo(parentCol), "inner")
            .drop(parentCol)
    }
}
