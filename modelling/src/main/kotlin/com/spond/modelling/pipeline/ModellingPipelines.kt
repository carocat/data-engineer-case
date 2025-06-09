package com.spond.modelling.pipeline

import com.spond.common.models.Team
import com.spond.common.models.Event
import com.spond.common.models.EventRsvp
import com.spond.common.models.Membership
import com.spond.modelling.transformers.IntegrityFKTransformer
import com.spond.modelling.transformers.RsvpStatusTransformer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Defines transformation pipelines for each entity ingested into the data lake.
 * Each pipeline applies data integrity checks and relevant transformations.
 */
object ModellingPipelines {


    /**
     * Pipeline to transform Membership dataset.
     * Applies foreign key integrity validation against Teams dataset.
     * Using a Pipeline with a single transformer may be verbose but allows future extensibility.
     */    fun transformMemberships(df: Dataset<Membership>, teams: Dataset<Team>): Dataset<Row> {
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

    /**
     * Pipeline to transform Events dataset.
     * Validates foreign key integrity for team references using Teams dataset.
     * Same design as transformMemberships for consistency and extendibility.
     */
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

    /**
     * Pipeline to transform EventRsvp dataset.
     * Applies multiple integrity checks for eventId and membershipId foreign keys.
     * Also applies RSVP status transformation for downstream analysis.
     */
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
                ),
                RsvpStatusTransformer()
            )
        )
        return pipeline.fit(df).transform(df)
    }
}
