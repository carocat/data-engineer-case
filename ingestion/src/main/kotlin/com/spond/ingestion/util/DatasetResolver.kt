package com.spond.ingestion.util

import com.spond.common.loader.DatasetLoader
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object DatasetResolver {

    /**
     * Loads all datasets from the resources directory using Spark.
     * @param spark SparkSession used to read the datasets.
     * @return A map of dataset names to their corresponding Spark Datasets.
     */
    fun loadAll(spark: SparkSession): Map<String, Dataset<Row>> {
        fun resolveResource(path: String): String =
            DatasetResolver::class.java.classLoader.getResource(path)
                ?.toURI()
                ?.toString()
                ?: throw IllegalArgumentException("Resource $path not found in classpath.")

        return mapOf(
            "teams" to DatasetLoader.loadCsv(spark, resolveResource("data/teams.csv"), Schemas.teamSchema),
            "memberships" to DatasetLoader.loadCsv(spark, resolveResource("data/memberships.csv"), Schemas.membershipSchema),
            "events" to DatasetLoader.loadCsv(spark, resolveResource("data/events.csv"), Schemas.eventSchema),
            "eventRsvps" to DatasetLoader.loadCsv(spark, resolveResource("data/event_rsvps.csv"), Schemas.eventRsvpSchema)
        )
    }
}