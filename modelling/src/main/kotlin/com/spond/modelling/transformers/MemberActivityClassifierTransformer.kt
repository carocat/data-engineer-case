package com.spond.modelling.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.weekofyear
import org.apache.spark.sql.functions.`when`
import org.apache.spark.sql.functions.year

class MemberActivityClassifierTransformer(
    private val eventsRsvpDs: Dataset<Row>,
    uid: String = Identifiable.randomUID("MemberActivityClassifierTransformer")
) : BaseTransformer(uid) {

    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {

        // 1. Extract join week and year from the 'memberships' data
        // This ensures we know when each member first joined
        val memberships = dataset.select("membershipId", "joinedAt")
            .dropDuplicates("membershipId")
            .withColumn("join_week", weekofyear(col("joinedAt")))
            .withColumn("join_year", year(col("joinedAt")))

        // 2. Extract RSVP activity week and year
        // This captures each week a member was active (RSVPed)
        val activity = eventsRsvpDs.select("membershipId", "respondedAt")
            .withColumn("activity_week", weekofyear(col("respondedAt")))
            .withColumn("activity_year", year(col("respondedAt")))
            .dropDuplicates("membershipId", "activity_week", "activity_year")

        // 3. Join activity with membership join info
        val joined = activity.join(memberships, "membershipId")

        // 4. Classify each activity as 'new' or 'returning'
        // A member is considered "new" in the week they joined, "returning" otherwise
        val classified = joined.withColumn(
            "status",
            `when`(
                col("activity_week").equalTo(col("join_week"))
                    .and(col("activity_year").equalTo(col("join_year"))),
            "new"
        ).otherwise("returning")
        )

        // 5. Aggregate by activity week and year, and count unique members per status
        return classified.groupBy("activity_year", "activity_week", "status")
            .agg(countDistinct("membershipId").alias("member_count"))
            .orderBy("activity_year", "activity_week", "status")
    }

}
