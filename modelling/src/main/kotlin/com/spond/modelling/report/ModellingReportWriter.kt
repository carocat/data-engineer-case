package com.spond.modelling.report

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.File

object ViewReportGenerator {

    private val logger = LoggerFactory.getLogger(ViewReportGenerator::class.java)

    /**
     * Generates a report file that includes:
     * - Previews of SQL views registered in Spark with row counts.
     * - Sample rows and row counts from provided transformed datasets.
     *
     * @param spark SparkSession used to query views.
     * @param outputPath File path for the output report (default: "report/view_report.txt").
     * @param datasets Optional map of dataset labels to Dataset<Row> to preview in the report.
     */
    fun generateReport(
        spark: SparkSession,
        outputPath: String = "report/ModellingReport.txt",
        datasets: Map<String, Dataset<Row>> = emptyMap()
    ) {
        val viewToLabel = mapOf(
            "vw_daily_team_activity" to "📅 Daily Team Activity",
            "vw_rsvp_summary_per_event_day" to "📨 RSVP Summary per Day",
            "vw_event_attendance_rate" to "✅ Event Attendance Rate",
            "vw_weekly_member_status" to "🧑 Weekly Member Status"
        )

        val outputFile = File(outputPath)
        outputFile.parentFile?.apply {
            if (!exists()) {
                logger.info("Creating directories: ${absolutePath}")
                mkdirs()
            }
        }

        logger.info("Generating report at: ${outputFile.absolutePath}")

        outputFile.printWriter().use { writer ->

            // Write dataset samples
            if (datasets.isNotEmpty()) {
                writer.println("=== Transformed Dataset Samples ===")
                writer.println()
                datasets.forEach { (label, ds) ->
                    writer.println("== $label ==")
                    try {
                        logger.info("Showing sample rows for dataset: $label")
                        val count = ds.count()
                        writer.println("Row count: $count")
                        val sample = ds.showString(5, 0, false)
                        writer.println(sample)
                    } catch (e: Exception) {
                        logger.warn("Failed to show sample rows for dataset: $label", e)
                        writer.println("⚠️  Failed to show sample rows.")
                    }
                    writer.println()
                }
            }

            // Write SQL view previews
            viewToLabel.forEach { (viewName, label) ->
                writer.println("== $label ($viewName) ==")
                try {
                    logger.info("Fetching preview and row count for view: $viewName")
                    val df = spark.sql("SELECT * FROM $viewName")
                    val count = df.count()
                    writer.println("Row count: $count")
                    val preview = df.limit(5).showString(5, 0, false)
                    writer.println(preview)
                } catch (e: Exception) {
                    logger.warn("Failed to preview view: $viewName", e)
                    writer.println("⚠️  Failed to preview this view.")
                }
                writer.println()
            }
        }

        logger.info("Report generation complete.")
    }
}
