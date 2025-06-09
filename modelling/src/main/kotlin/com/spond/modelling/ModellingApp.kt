package com.spond.modelling


import com.spond.common.models.Event
import com.spond.common.models.EventRsvp
import com.spond.common.models.Membership
import com.spond.common.models.Team
import com.spond.common.spark.SparkSessionManager
import com.spond.modelling.pipeline.ModellingPipelines
import com.spond.modelling.report.ViewReportGenerator
import com.spond.modelling.views.AnalyticsViews
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

object ModellingApp {

    /**
     * Entry point for the validation job.
     * Manages Spark session lifecycle, runs foreign key validations, and reports inconsistencies.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        SparkSessionManager.withSpark("ModellingApp") { spark ->


            val memberships: Dataset<Membership> = spark.read()
                .format("delta").load("../data/output/memberships.delta")
                .`as`(Encoders.bean(Membership::class.java))

            val teams: Dataset<Team> = spark.read()
                .format("delta").load("../data/output/teams.delta")
                .`as`(Encoders.bean(Team::class.java))

            val events: Dataset<Event> = spark.read()
                .format("delta").load("../data/output/events.delta")
                .`as`(Encoders.bean(Event::class.java))

            val eventRsvps: Dataset<EventRsvp> = spark.read()
                .format("delta").load("../data/output/event_rsvps.delta")
                .`as`(Encoders.bean(EventRsvp::class.java))

            // NOTE: While we're applying transformation pipelines here for demonstration purposes,
            // in a real-world scenario, this step would typically belong in the ingestion layer.
            //
            // It's also critical to persist any rows removed by IntegrityFKTransformer (e.g. invalid foreign keys)
            // to a separate debug/audit sink (e.g. Delta table, log file, or monitoring system)
            // to support traceability, data quality reviews, and operational debugging.
            val transformedMemberships = ModellingPipelines.transformMemberships(memberships, teams)
            val transformedEvents = ModellingPipelines.transformEvents(events, teams)
            val transformedEventRsvps = ModellingPipelines.transformEventRsvps(eventRsvps, events, memberships)

            AnalyticsViews.createAllViews(
                teams = teams.toDF(),
                memberships = transformedMemberships,
                events = transformedEvents,
                eventRsvps = transformedEventRsvps
            )

            // Generate report
            ViewReportGenerator.generateReport(
                spark = spark,
                datasets = mapOf(
                    "Transformed Teams" to teams.toDF(),
                    "Transformed Memberships" to transformedMemberships,
                    "Transformed Events" to transformedEvents,
                    "Transformed Event RSVPs" to transformedEventRsvps
                )
            )

        }
    }
}