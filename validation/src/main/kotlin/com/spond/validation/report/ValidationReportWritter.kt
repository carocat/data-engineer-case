package com.spond.validation.report

import org.apache.spark.sql.Dataset
import org.slf4j.LoggerFactory
import java.io.File

data class ValidationResult(val name: String, val passed: Boolean, val invalidCount: Long)

object ValidationReport {
    private val logger = LoggerFactory.getLogger(ValidationReport::class.java)

    /**
     * Validate and log, returns ValidationResult for later summary writing.
     */
    fun <T> reportValidation(name: String, dataset: Dataset<T>): ValidationResult {
        val count = dataset.count()
        return if (count == 0L) {
            logger.info("✅ [Validation PASSED] $name: no invalid records found")
            ValidationResult(name, true, 0)
        } else {
            logger.error("❌ [Validation FAILED] $name: $count invalid records found")
            dataset.show(10, false)
            ValidationResult(name, false, count)
        }
    }

    /**
     * Write summary report to a text file.
     */
    fun writeSummaryReport(summary: String, outputPath: String) {
        val file = File(outputPath)

        // Log the absolute path
        logger.info("\uD83D\uDE80\uD83D\uDE80\uD83D\uDE80 Writing validation summary to: ${file.absolutePath}\uD83D\uDE80\uD83D\uDE80\uD83D\uDE80")

        // Ensure parent directory exists
        file.parentFile?.let {
            if (!it.exists()) {
                val created = it.mkdirs()
                if (created) {
                    logger.info("\uD83D\uDCC1\uD83D\uDCC1\uD83D\uDCC1 Created directories: ${it.absolutePath} \uD83D\uDCC1\uD83D\uDCC1\uD83D\uDCC1 ")
                } else {
                    logger.warn("⚠\uFE0F⚠\uFE0F⚠\uFE0F Failed to create directories: ${it.absolutePath}⚠\uFE0F⚠\uFE0F⚠\uFE0F ")
                }
            }
        }

        // Write summary text to file
        file.writeText(summary)

        logger.info("✅✅✅ Validation summary report written successfully.✅✅✅ ")
    }

}
