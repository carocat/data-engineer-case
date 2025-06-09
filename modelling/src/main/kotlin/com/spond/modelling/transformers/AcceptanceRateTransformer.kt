package com.spond.modelling.transformers

import com.spond.common.enums.RsvpStatus
import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.`when`
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.date_sub
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.round

// This transformer calculates the attendance rate (% of accepted RSVPs)
// It keeps only the latest RSVP per (eventId, membershipId) pair
class AcceptanceRateTransformer(
    uid: String = Identifiable.randomUID("AcceptanceRateTransformer")
) : BaseTransformer(uid) {

    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {

        // Get the latest RSVP creation date in the dataset.
        // This is used as the reference point to define the "last 30 days".
        // Step 1: Compute the max respondedAt date (as a Dataset<Row>)
        val maxDateDF = dataset
            .select(max(to_date(col("respondedAt"))).alias("lastDay"))

        // Step 2: Cross join original dataset with the max date
        val withLastDay = dataset.crossJoin(maxDateDF)

        // Step 2: Filter using lastDay as a literal
        val filtered = withLastDay.filter(
            to_date(col("respondedAt")).between(
                date_sub(col("lastDay"), 30),
                col("lastDay")
            )
        )
        // Step 1: Keep only the latest RSVP per (eventId, membershipId)
        val deduped = filtered
            .withColumn(
                "rnk",
                row_number().over(
                    Window
                        .partitionBy("eventId", "membershipId") // Partition by each (event, member) pair
                        .orderBy(col("year").desc(), col("month").desc(), col("day").desc())     // Order by most recent RSVP first
                )
            )
            .filter(col("rnk").equalTo(1)) // Keep only the latest RSVP record for each invite

        // I assume that:
        // - A single member can be invited to multiple different events.
        // - A member's RSVP status may change over time (e.g., unanswered → accepted or declined).
        // To avoid double-counting or outdated responses, we:
        // - Deduplicate the dataset by keeping only the most recent RSVP per (eventId, membershipId).
        // - This ensures we calculate the attendance rate using each member's latest intent per event
        //   in the last 30 days.


        // Step 2: Aggregate total invites and accepted RSVPs
        return deduped
            .agg(
                count("*").alias("total_invites"), // total unique invites
                count(
                    `when`(col("rsvpStatus").equalTo(RsvpStatus.ACCEPTED.name.lowercase()), true)
                ).alias(RsvpStatus.ACCEPTED.name.lowercase()) // total accepted
            )
            // Step 3: Calculate attendance rate (% accepted out of total)
            .withColumn(
                "attendance_rate",
                round(
                    col(RsvpStatus.ACCEPTED.name.lowercase())
                        .multiply(lit(100))
                        .divide(col("total_invites")),
                    2
                )
            )
    }
}