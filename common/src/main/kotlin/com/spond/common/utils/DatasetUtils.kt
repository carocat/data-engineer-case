package com.spond.common.utils

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object DatasetUtils {

    /**
     * Validates that the specified columns in the dataset contain no null values.
     *
     * @param dataset The input dataset to validate.
     * @param requiredCols A list of column names that must not contain nulls.
     * @return The original dataset if all required columns are non-null.
     * @throws IllegalStateException if any of the required columns contain null values.
     */
    fun validateNonNulls(dataset: Dataset<Row>, requiredCols: List<String>): Dataset<Row> {
        requiredCols.forEach { col ->
            val nullCount = dataset.filter(dataset.col(col).isNull).count()
            if (nullCount > 0) {
                throw IllegalStateException("Null values found in required column '$col'")
            }
        }
        return dataset
    }

}
