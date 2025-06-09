package com.spond.validation.check

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions

/**
 * Returns rows from the dataset that have duplicate values on the specified primary key columns.
 * These rows violate uniqueness constraints.
 */
object UniquenessValidator {
    fun <T> nonUniqueRows(
        df: Dataset<T>,
        key: String,
        encoder: Encoder<T>
    ): Dataset<T> {
        require(df.columns().contains(key)) {
            "❌ Unique key column '$key' not found in dataset."
        }

        val duplicateKeys = df.groupBy(functions.col(key))
            .count()
            .filter("count > 1")
            .drop("count")

        return df.join(duplicateKeys, arrayOf(key), "inner")
            .`as`(encoder)
    }
}