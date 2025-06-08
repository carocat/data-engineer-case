package com.spond.common.delta

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * Utility for writing Spark DataFrames in Delta format.
 *
 * - If the input DataFrame contains 'year', 'month', and 'day' columns,
 *   it is automatically partitioned by those columns.
 * - Delta is used for all writes to support ACID guarantees and efficient querying.
 * - The Delta table is registered as a temporary view for SQL use.
 *
 * @param df the input DataFrame
 * @param tableName the name to register in Spark SQL
 * @param deltaPath the path where the Delta table should be saved
 * @param spark the active SparkSession
 */
object DeltaWriter {

    fun writeAndRegister(
        df: Dataset<Row>,
        tableName: String,
        deltaPath: String,
        spark: SparkSession
    ) {
        val cols = df.columns().toSet()
        val shouldPartition = cols.contains("year") && cols.contains("month") && cols.contains("day")

        val deltaWriter = df.write()
            .format("delta")
            .mode(SaveMode.Overwrite)

        if (shouldPartition) {
            deltaWriter.partitionBy("year", "month", "day")
        }

        deltaWriter.save(deltaPath)

        spark.read()
            .format("delta")
            .load(deltaPath)
            .createOrReplaceTempView(tableName)
    }
}
