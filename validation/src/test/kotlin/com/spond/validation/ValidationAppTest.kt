package com.spond.validation

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.*

/**
 * Integration-style test for manually running the `ValidationApp.main()` method inside IntelliJ.
 *
 * This test is not meant for production or CI use but instead serves as a convenient way
 * to run and debug the validation pipeline using a local Spark session.
 *
 * The Spark session is created with `local[*]` master to enable parallel execution on all available cores.
 *
 * Note:
 * - No assertions are made in this test.
 * - Useful during development for visual validation of output, logs, or debugging.
 */
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class ValidationAppTest {

    private lateinit var spark: SparkSession

    @BeforeEach
    fun setup() {
        spark = SparkSession.builder()
            .appName("Test")
            .master("local[*]")
            .getOrCreate()
    }

    @AfterEach
    fun teardown() {
        if (!spark.sparkContext().isStopped) {
            spark.stop()
        }
    }

    /**
     * Runs the main validation pipeline entrypoint.
     *
     * This simulates a real run and allows the developer to observe behavior,
     * logs, and outputs within the IDE environment.
     */
    @Test
    fun `run validation main successfully`() {
        ValidationApp.main(emptyArray())
    }
}
