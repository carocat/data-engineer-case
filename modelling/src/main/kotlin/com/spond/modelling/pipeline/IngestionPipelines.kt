package com.spond.modelling.pipeline

import com.spond.common.models.Team
import com.spond.common.models.Event
import com.spond.common.models.EventRsvp
import com.spond.common.models.Membership
import com.spond.modelling.transformers.IntegrityFKTransformer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Defines transformation pipelines for each entity ingested into the data lake.
 */
object IngestionPipelines {


    //matter of choice to use a pipeline just for one transformer, it is a little verbose indeed
    fun transformMemberships(df: Dataset<Membership>, teams: Dataset<Team>): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                IntegrityFKTransformer(
                    parent = teams,
                    parentKeyCol = "teamId",
                    childKeyCol = "teamId",
                )
            )
        )
        return pipeline.fit(df).transform(df)
    }

    fun transformEvents(df: Dataset<Event>, teams: Dataset<Team>): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                IntegrityFKTransformer(
                    parent = teams,
                    parentKeyCol = "teamId",
                    childKeyCol = "teamId",
                )
            )
        )
        return pipeline.fit(df).transform(df)
    }

    fun transformEventRsvps(
        df: Dataset<EventRsvp>,
        events: Dataset<Event>,
        memberships: Dataset<Membership>
    ): Dataset<Row> {
        val pipeline = Pipeline().setStages(
            arrayOf(
                IntegrityFKTransformer(
                    parent = events,
                    parentKeyCol = "eventId",
                    childKeyCol = "eventId",
                ),
                IntegrityFKTransformer(
                    parent = memberships,
                    parentKeyCol = "membershipId",
                    childKeyCol = "membershipId",
                )
            )
        )
        return pipeline.fit(df).transform(df)
    }
}
