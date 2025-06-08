package com.spond.ingestion

import com.spond.common.delta.DeltaWriter
import com.spond.common.spark.SparkSessionManager
import com.spond.ingestion.encoding.EventEncode
import com.spond.ingestion.encoding.EventRsvpEncode
import com.spond.ingestion.encoding.MembershipEncode
import com.spond.ingestion.encoding.TeamEncode
import com.spond.ingestion.pipeline.IngestionPipelines
import com.spond.ingestion.util.DatasetLogger
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
            DeltaWriter.writeAndRegister(
                df = teamsDs.toDF(),
                tableName = "teams",
                deltaPath = "../data/output/teams.delta",
                spark = spark
            )

            DeltaWriter.writeAndRegister(
                df = membershipsDs.toDF(),
                tableName = "memberships",
                deltaPath = "../data/output/memberships.delta",
                spark = spark
            )

            // Non-partitioned writes for events and event_rsvps (partitioning can be added if needed)
            DeltaWriter.writeAndRegister(
                df = eventsDs.toDF(),
                tableName = "events",
                deltaPath = "../data/output/events.delta",
                spark = spark
            )

            DeltaWriter.writeAndRegister(
                df = eventRsvpsDs.toDF(),
                tableName = "event_rsvps",
                deltaPath = "../data/output/event_rsvps.delta",
                spark = spark
            )

            // === Log schemas and sample data for debug and verification ===
            DatasetLogger.logAndShow(teamsDs, "Teams")
            DatasetLogger.logAndShow(membershipsDs, "Memberships")
            DatasetLogger.logAndShow(eventsDs, "Events")
            DatasetLogger.logAndShow(eventRsvpsDs, "Event RSVPs")
        }
    }
}
