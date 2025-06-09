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
    // Only select the parent key column to avoid redundancy or name conflicts.
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        val childCol = dataset.col(childKeyCol)
        val minimalParent = parent.select(parent.col(parentKeyCol))  // Select only key column

        // Use broadcast join to improve join performance when minimalParent is small enough to fit in memory.
        // Broadcasting avoids a costly shuffle of the minimalParent dataset across the cluster,
        // reducing network I/O and speeding up the join operation.
        return dataset
            .join(broadcast(minimalParent), childCol.equalTo(minimalParent.col(parentKeyCol)))
            .drop(minimalParent.col(parentKeyCol))
    }
}
