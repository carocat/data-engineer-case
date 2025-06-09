package com.spond.validation

import com.spond.common.models.Event
import com.spond.common.models.EventRsvp
import com.spond.common.models.Membership
import com.spond.common.models.Team
import com.spond.common.spark.SparkSessionManager
import com.spond.validation.check.ForeignKeyValidator
import com.spond.validation.check.UniquenessValidator
import com.spond.validation.report.ValidationReport
import com.spond.validation.report.ValidationResult
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import java.time.LocalDateTime

object ValidationApp {

    /**
     * Entry point for the validation job.
     * Manages Spark session lifecycle, runs foreign key validations, and reports inconsistencies.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        SparkSessionManager.withSpark("ValidationApp") { spark ->
            val membershipEncoder: Encoder<Membership> = Encoders.bean(Membership::class.java)
            val teamEncoder: Encoder<Team> = Encoders.bean(Team::class.java)
            val eventEncoder: Encoder<Event> = Encoders.bean(Event::class.java)
            val eventRsvpEncoder: Encoder<EventRsvp> = Encoders.bean(EventRsvp::class.java)

            // Load datasets from Delta
            val memberships: Dataset<Membership> = spark.read()
                .format("delta").load("../data/output/memberships.delta")
                .`as`(membershipEncoder)

            val teams: Dataset<Team> = spark.read()
                .format("delta").load("../data/output/teams.delta")
                .`as`(teamEncoder)

            val events: Dataset<Event> = spark.read()
                .format("delta").load("../data/output/events.delta")
                .`as`(eventEncoder)

            val eventRsvps: Dataset<EventRsvp> = spark.read()
                .format("delta").load("../data/output/event_rsvps.delta")
                .`as`(eventRsvpEncoder)

            // Run foreign key validation checks
            val membershipsWithoutTeams = ForeignKeyValidator.membershipsWithoutTeams(memberships, teams)
            val eventsWithoutMemberships = ForeignKeyValidator.eventsWithoutMemberships(events, memberships)
            val eventRsvpsWithoutEvents = ForeignKeyValidator.eventRsvpsWithoutEvents(eventRsvps, events)
            val eventRsvpsWithoutMemberships = ForeignKeyValidator.eventRsvpsWithoutMemberships(eventRsvps, memberships)

            // Run unique key validation
            val teamsAreUnique = UniquenessValidator.nonUniqueRows(teams, "teamId", teamEncoder)
            val membershipsAreUnique = UniquenessValidator.nonUniqueRows(memberships, "membershipId", membershipEncoder)
            val eventsAreUnique = UniquenessValidator.nonUniqueRows(events, "eventId", eventEncoder)
            val rsvpsAreUnique = UniquenessValidator.nonUniqueRows(eventRsvps, "eventRsvpId", eventRsvpEncoder)

            val results = mutableListOf<ValidationResult>()
            results += ValidationReport.reportValidation("Memberships without Teams", membershipsWithoutTeams)
            results += ValidationReport.reportValidation("Events without Memberships", eventsWithoutMemberships)
            results += ValidationReport.reportValidation("Event RSVPs without Events", eventRsvpsWithoutEvents)
            results += ValidationReport.reportValidation("Event RSVPs without Memberships", eventRsvpsWithoutMemberships)
            // Primary key uniqueness checks
            results += ValidationReport.reportValidation("Teams PK Uniqueness", teamsAreUnique)
            results += ValidationReport.reportValidation("Memberships PK Uniqueness", membershipsAreUnique)
            results += ValidationReport.reportValidation("Events PK Uniqueness", eventsAreUnique)
            results += ValidationReport.reportValidation("Event RSVPs PK Uniqueness", rsvpsAreUnique)


            // Convert the list of ValidationResult to a formatted String summary
            val summaryText = buildString {
                appendLine("=========================================")
                appendLine("🧪 Spond Data Validation Summary Report")
                appendLine("Generated at: ${LocalDateTime.now()}")
                appendLine("=========================================\n")

                results.forEach { result ->
                    appendLine("🔍 Validation: ${result.name}")
                    if (result.passed) {
                        appendLine("✅ PASSED - No invalid records found\n")
                    } else {
                        appendLine("❌ FAILED - ${result.invalidCount} invalid records found")
                        appendLine()
                    }
                    appendLine("-----------------------------------------\n")
                }

                appendLine("✅ End of Validation Report ✅")
            }

            // Write the summary report to a file
            ValidationReport.writeSummaryReport(summaryText, "report/ValidationReport.txt")
        }
    }

}
