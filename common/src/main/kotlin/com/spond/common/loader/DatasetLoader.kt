package com.spond.common.loader

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Utility object for loading datasets into Spark.
 */
object DatasetLoader {

    /**
     * Loads a CSV file into a Spark [Dataset] of [Row]s using a predefined schema.
     *
     * @param spark The active [SparkSession].
     * @param path Path to the CSV file.
     * @param schema The schema to apply while reading the CSV.
     * @return A [Dataset] containing the rows of the CSV file, with header and specified schema.
     */
    fun loadCsv(spark: SparkSession, path: String, schema: StructType): Dataset<Row> =
        spark.read()
            .option("header", "true")
            .option("delimiter", ",")
            .schema(schema)
            .csv(path)
}
