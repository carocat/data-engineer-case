package com.spond.common.spark

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Utility object for managing the lifecycle of a SparkSession.
 */
object SparkSessionManager {
    private val logger = LoggerFactory.getLogger(SparkSessionManager::class.java)

    /**
     * Runs the provided block with a SparkSession and ensures the session is stopped afterwards.
     *
     * @param appName Name of the Spark application.
     * @param block Code block to execute using the SparkSession.
     */
    fun withSpark(appName: String, block: (SparkSession) -> Unit) {
        val spark = SparkSessionFactory.getLocalSession(appName)
        try {
            logger.info("Spark session [$appName] started.")
            block(spark)
        } finally {
            logger.info("Stopping Spark session [$appName].")
            spark.stop()
        }
    }
}

