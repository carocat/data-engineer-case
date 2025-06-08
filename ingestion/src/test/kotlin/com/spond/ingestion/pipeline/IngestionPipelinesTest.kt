package com.spond.ingestion.pipeline

import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import kotlin.test.assertTrue
import java.sql.Timestamp


/**
 * These tests are designed to ensure that the Spark logical plan
 * for each pipeline is triggered and executed correctly. We simulate the
 * full execution path by providing minimal valid input rows, and assert
 * that the resulting Dataset is not empty after the transformation.
 *
 * The focus is on verifying that the entire pipeline logic is wired up
 * correctly, including all relevant transformations, filters, projections,
 * and derived columns.
 */
class IngestionPipelinesTest {

    companion object {
        private lateinit var spark: SparkSession

        @BeforeAll
        @JvmStatic
        fun setup() {
            spark = SparkSession.builder()
                .appName("IngestionPipelinesTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            spark.stop()
        }
    }

    @Test
    fun `transformTeams should return non-empty dataset for valid input`() {
        val data = listOf(
            RowFactory.create(
                1,
                " High Intensity ",
                "NO",
                Timestamp.valueOf("2024-01-01 10:00:00")
            )
        )

        val schema = StructType()
            .add("team_id", DataTypes.IntegerType)
            .add("team_activity", DataTypes.StringType)
            .add("country_code", DataTypes.StringType)
            .add("created_at", DataTypes.TimestampType)

        val df = spark.createDataFrame(data, schema)
        val result = IngestionPipelines.transformTeams(df)

        assertTrue(result.count() > 0, "Transformed teams dataset should not be empty")
    }

    @Test
    fun `transformMemberships should return non-empty dataset for valid input`() {
        val data = listOf(
            RowFactory.create(
                1,
                " Captain ",
                Timestamp.valueOf("2024-01-01 10:00:00")
            )
        )

        val schema = StructType()
            .add("group_id", DataTypes.IntegerType)
            .add("role_title", DataTypes.StringType)
            .add("joined_at", DataTypes.TimestampType)

        val df = spark.createDataFrame(data, schema)
        val result = IngestionPipelines.transformMemberships(df)

        assertTrue(result.count() > 0, "Transformed memberships dataset should not be empty")
    }

    @Test
    fun `transformEvents should return non-empty dataset for valid input`() {
        val data = listOf(
            RowFactory.create(
                "e1",
                "t1",
                Timestamp.valueOf("2024-01-01 12:00:00"),
                Timestamp.valueOf("2024-01-01 13:00:00"),
                59.91,
                10.75,
                Timestamp.valueOf("2024-01-01 10:00:00")
            )
        )

        val schema = StructType()
            .add("event_id", DataTypes.StringType)
            .add("team_id", DataTypes.StringType)
            .add("event_start", DataTypes.TimestampType)
            .add("event_end", DataTypes.TimestampType)
            .add("latitude", DataTypes.DoubleType)
            .add("longitude", DataTypes.DoubleType)
            .add("created_at", DataTypes.TimestampType)

        val df = spark.createDataFrame(data, schema)
        val result = IngestionPipelines.transformEvents(df)

        assertTrue(result.count() > 0, "Transformed events dataset should not be empty")
    }

    @Test
    fun `transformEventRsvps should return non-empty dataset for valid input`() {
        val data = listOf(
            RowFactory.create(
                "r1",
                "e1",
                "m1",
                1,
                Timestamp.valueOf("2024-01-01 12:30:00")
            )
        )

        val schema = StructType()
            .add("event_rsvp_id", DataTypes.StringType)
            .add("event_id", DataTypes.StringType)
            .add("membership_id", DataTypes.StringType)
            .add("rsvp_status", DataTypes.IntegerType)
            .add("responded_at", DataTypes.TimestampType)

        val df = spark.createDataFrame(data, schema)
        val result = IngestionPipelines.transformEventRsvps(df)

        assertTrue(result.count() > 0, "Transformed event_rsvps dataset should not be empty")
    }
}
