package com.spond.ingestion.report

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.desc
import org.slf4j.LoggerFactory
import java.io.File

object IngestionReportGenerator {

    private val logger = LoggerFactory.getLogger(IngestionReportGenerator::class.java)

    fun generateReport(
        teams: Dataset<Row>,
        memberships: Dataset<Row>,
        events: Dataset<Row>,
        eventRsvps: Dataset<Row>,
        outputPath: String = "report/IngestionReport.txt"
    ) {
        val outputFile = File(outputPath)

        // Ensure parent directory exists
        outputFile.parentFile?.mkdirs()

        logger.info("Generating ingestion report at: ${outputFile.absolutePath}")

        outputFile.printWriter().use { writer ->
            writeDatasetSection(writer, "✅ Teams", teams)
            writeDatasetSection(writer, "🧑 Memberships", memberships)
            writeDatasetSection(writer, "📅 Events", events)
            writeDatasetSection(writer, "📨 Event RSVPs", eventRsvps)
        }

        logger.info("Ingestion report generation complete.")
    }

    private fun writeDatasetSection(writer: java.io.PrintWriter, label: String, ds: Dataset<Row>) {
        try {
            val count = ds.count()
            writer.println("== $label ==")
            writer.println("Row count: $count")
            writer.println("Schema:")
            writer.println(ds.schema().treeString())
            writer.println("Sample Data:")
            writer.println(ds
                .orderBy(desc("year"), desc("month"), desc("day"))
                .showString(5, 0, false)
            )
        } catch (e: Exception) {
            writer.println("== $label ==")
            writer.println("⚠️  Failed to process dataset.")
        }
        writer.println()
    }
}
