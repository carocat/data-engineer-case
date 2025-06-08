package com.spond.ingestion.pipeline

import com.spond.common.transformers.AddDatePartitionsTransformer
import com.spond.ingestion.transformers.DropDuplicatesTransformer
import com.spond.ingestion.transformers.RemoveNullsTransformer
import com.spond.ingestion.transformers.RemoveSpacesTransformer
import com.spond.ingestion.transformers.TimeStampTransformer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Defines transformation pipelines for each entity ingested into the data lake.
 *
 * Each transformation applies a series of custom Spark MLlib `Transformer`s to clean,
 * enrich, and standardize the input data before it is persisted or validated.
 */
object IngestionPipelines {

    /**
     * Applies a transformation pipeline to the Teams dataset.
     *
     * Steps:
     * - Removes extra spaces in the "team_activity" column.
     * - Drops duplicate rows.
     * - Adds partition columns (year, month, day) based on "created_at".
     *
     * @param df Raw input dataset for teams.
     * @return Transformed dataset.
     */
    fun transformTeams(df: Dataset<Row>): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                //TimeStampTransformer("created_at"), //not used because I didn't face issues but that would be my approach to check malformed timestamps:
                // setting the column as String and then using this transformer
                RemoveSpacesTransformer("team_activity"),//handle spaces if it exists
                DropDuplicatesTransformer(),
                AddDatePartitionsTransformer("created_at")//partition in case it is a daily batch job so it's relevant
            )
        )
        val model = pipeline.fit(df)
        return model.transform(df)
    }

    /**
     * Applies a transformation pipeline to the Memberships dataset.
     *
     * Steps:
     * - Replaces nulls with zero in "group_id".
     * - Removes extra spaces in the "role_title" column.
     * - Drops duplicate rows.
     * - Adds partition columns (year, month, day) based on "joined_at".
     *
     * @param df Raw input dataset for memberships.
     * @return Transformed dataset.
     */
    fun transformMemberships(df: Dataset<Row>): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                RemoveSpacesTransformer("role_title"),
                DropDuplicatesTransformer(),
                AddDatePartitionsTransformer("joined_at")
            )
        )
        val model = pipeline.fit(df)
        return model.transform(df)
    }

    /**
     * Applies a transformation pipeline to the Events dataset.
     *
     * Steps:
     * - Drops duplicate rows.
     * - Adds partition columns (year, month, day) based on "created_at".
     *
     * @param df Raw input dataset for events.
     * @return Transformed dataset.
     */
    fun transformEvents(df: Dataset<Row>): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                TimeStampTransformer("event_end"),
                TimeStampTransformer("event_start"),
                DropDuplicatesTransformer(),
                AddDatePartitionsTransformer("created_at")
            )
        )
        val model = pipeline.fit(df)
        return model.transform(df)
    }

    /**
     * Applies a transformation pipeline to the Event RSVPs dataset.
     *
     * Steps:
     * - Drops duplicate rows.
     * - Adds partition columns (year, month, day) based on "responded_at".
     *
     * @param df Raw input dataset for event RSVPs.
     * @return Transformed dataset.
     */
    fun transformEventRsvps(df: Dataset<Row>): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                DropDuplicatesTransformer(),
                AddDatePartitionsTransformer("responded_at"),
                RemoveNullsTransformer("event_rsvp_id") //shouldn't really do this, not like this at least, we can talk about it
            )
        )
        val model = pipeline.fit(df)
        return model.transform(df)
    }
}
