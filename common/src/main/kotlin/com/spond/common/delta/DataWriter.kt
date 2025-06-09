package com.spond.common.delta

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import java.io.File

/**
 * Writes a DataFrame in both Delta and Parquet formats.
 *
 * - Delta format goes to the given `deltaPath`.
 * - Parquet format goes to a sibling folder with `.parquet` suffix.
 * - Partitions by year/month/day if those columns exist.
 * - Registers the Delta table as a temp view in Spark.
 */
object DataWriter {

    fun writeDeltaAndParquet(
        df: Dataset<Row>,
        tableName: String,
        deltaPath: String,
        spark: SparkSession
    ) {
        val cols = df.columns().toSet()
        val shouldPartition = cols.contains("year") && cols.contains("month") && cols.contains("day")

        val parquetPath = deltaPath.replaceAfterLast("/", File(deltaPath).nameWithoutExtension + ".parquet")

        val deltaWriter = df.write()
            .format("delta")
            .mode(SaveMode.Overwrite)

        val parquetWriter = df.write()
            .format("parquet")
            .mode(SaveMode.Overwrite)

        if (shouldPartition) {
            deltaWriter.partitionBy("year", "month", "day")
            parquetWriter.partitionBy("year", "month", "day")
        }

        deltaWriter.save(deltaPath)
        parquetWriter.save(parquetPath)

        spark.read()
            .format("delta")
            .load(deltaPath)
            .createOrReplaceTempView(tableName)
    }
}
