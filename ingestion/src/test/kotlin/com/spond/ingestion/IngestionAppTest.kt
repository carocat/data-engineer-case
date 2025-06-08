package com.spond.ingestion

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Test

/**
 * Integration-style test to execute the [IngestionApp.main] method.
 *
 * This test is primarily used for local development and debugging within IntelliJ,
 * but it is also configured to run during the build process to generate Delta files
 * under the output path defined in [IngestionApp].
 *
 * These Delta files are then consumed by the `validation` and `modelling` modules
 * for downstream testing and data modeling logic.
 *
 * This test is not intended as a traditional unit test or for CI environments
 * unless explicitly configured to support local file outputs.
 */
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class IngestionAppTest {

    private lateinit var spark: SparkSession

    /**
     * Initializes a local Spark session before each test case.
     */
    @BeforeEach
    fun setup() {
        spark = SparkSession.builder()
            .appName("Test")
            .master("local[*]")
            .getOrCreate()
    }

    /**
     * Stops the Spark session after each test case.
     */
    @AfterEach
    fun teardown() {
        if (!spark.sparkContext().isStopped) {
            spark.stop()
        }
    }

    /**
     * Runs the [IngestionApp.main] method to trigger the full ingestion pipeline.
     * Useful for verifying end-to-end execution and for generating required Delta outputs
     * used by validation and modeling modules in integration testing and development workflows.
     */
    @Test
    fun `run ingestion main successfully`() {
        IngestionApp.main(emptyArray())
    }
}
