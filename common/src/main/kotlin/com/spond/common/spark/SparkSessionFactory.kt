package com.spond.common.spark

import org.apache.spark.sql.SparkSession

/**
 * Factory object for creating a local SparkSession with Delta Lake support.
 */
object SparkSessionFactory {

    /**
     * Creates and returns a local SparkSession configured to support Delta Lake.
     *
     * @param appName Optional name of the Spark application (default: "DeltaLocal").
     * @return A SparkSession instance with Delta Lake extensions enabled.
     */
    fun getLocalSession(appName: String = "DeltaLocal"): SparkSession {
        return SparkSession.builder()
            .appName(appName)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    }
}

