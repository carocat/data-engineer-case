package com.spond.modelling.transformers

import com.spond.common.enums.RsvpStatus
import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.`when`
import org.apache.spark.sql.functions.count


class RsvpSummaryTransformer(
    private val inputCol: String = "rsvpStatus",
    uid: String = Identifiable.randomUID("RsvpSummaryTransformer")
) : BaseTransformer(uid) {

    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        return dataset
            .groupBy("eventId", "year", "month", "day")
            // Aggregate counts of RSVP statuses by conditionally counting rows matching each status
            .agg(
            count(`when`(col(inputCol).equalTo(RsvpStatus.ACCEPTED.name.lowercase()), true)).alias(RsvpStatus.ACCEPTED.name.lowercase()),
            count(`when`(col(inputCol).equalTo(RsvpStatus.DECLINED.name.lowercase()), true)).alias(RsvpStatus.DECLINED.name.lowercase()),
            count(`when`(col(inputCol).equalTo(RsvpStatus.UNANSWERED.name.lowercase()), true)).alias(RsvpStatus.UNANSWERED.name.lowercase()),
        ).na().fill(0L)
    }
}