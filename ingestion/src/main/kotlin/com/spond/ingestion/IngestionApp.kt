package com.spond.ingestion

import com.spond.common.delta.DataWriter
import com.spond.common.spark.SparkSessionManager
import com.spond.ingestion.encoding.EventEncode
import com.spond.ingestion.encoding.EventRsvpEncode
import com.spond.ingestion.encoding.MembershipEncode
import com.spond.ingestion.encoding.TeamEncode
import com.spond.ingestion.pipeline.IngestionPipelines
import com.spond.ingestion.report.IngestionReportGenerator
import com.spond.ingestion.util.DatasetResolver

object IngestionApp {

    /**
     * Entry point for the ingestion job.
     * Initializes Spark, loads and transforms datasets, writes Delta tables, and logs sample outputs.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        // Automatically creates and stops Spark session
        SparkSessionManager.withSpark("IngestionApp") { spark ->

            // === Load all input datasets from CSV ===
            val datasets = DatasetResolver.loadAll(spark)

            // === Domain-specific ETL transformations using pipelines ===
            // Apply custom MLlib-style transformers to clean and enrich each dataset
            val teamsDf = IngestionPipelines.transformTeams(datasets["teams"]!!)
            val membershipsDf = IngestionPipelines.transformMemberships(datasets["memberships"]!!)
            membershipsDf.show()
            val eventsDf = IngestionPipelines.transformEvents(datasets["events"]!!)
            eventsDf.show()
            val eventRsvpsDf = IngestionPipelines.transformEventRsvps(datasets["eventRsvps"]!!)
            eventRsvpsDf.show()

            // === Type-safe encoding of datasets Kotlin style ===
            // Converts DataFrames into strongly typed Datasets for safer transformations
            val teamsDs = TeamEncode.transform(teamsDf)
            val membershipsDs = MembershipEncode.transform(membershipsDf)
            val eventsDs = EventEncode.transform(eventsDf)
            val eventRsvpsDs = EventRsvpEncode.transform(eventRsvpsDf)

            // === Write and register Delta tables ===

            // Partitioned writes for teams and memberships (by year/month/day)
            DataWriter.writeDeltaAndParquet(
                df = teamsDs.toDF(),
                tableName = "teams",
                deltaPath = "../data/output/teams.delta",
                spark = spark
            )

            DataWriter.writeDeltaAndParquet(
                df = membershipsDs.toDF(),
                tableName = "memberships",
                deltaPath = "../data/output/memberships.delta",
                spark = spark
            )

            // Non-partitioned writes for events and event_rsvps (partitioning can be added if needed)
            DataWriter.writeDeltaAndParquet(
                df = eventsDs.toDF(),
                tableName = "events",
                deltaPath = "../data/output/events.delta",
                spark = spark
            )

            DataWriter.writeDeltaAndParquet(
                df = eventRsvpsDs.toDF(),
                tableName = "event_rsvps",
                deltaPath = "../data/output/event_rsvps.delta",
                spark = spark
            )

            // === Write a structured report instead of printing datasets to log ===
            IngestionReportGenerator.generateReport(
                teams = teamsDs.toDF(),
                memberships = membershipsDs.toDF(),
                events = eventsDs.toDF(),
                eventRsvps = eventRsvpsDs.toDF()
            )
        }
    }
}
