package com.spond.modelling.transformers

import com.spond.common.enums.RsvpStatus
import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.`when`

/**
 * A custom Spark ML transformer that standardizes RSVP status codes into lowercase string labels.
 *
 * This transformer maps numeric RSVP status codes (e.g., 0, 1, 2) to human-readable
 * status strings ("unanswered", "accepted", "declined"). It replaces the original
 * `rsvpStatus` column with the corresponding label.
 *
 * @property inputCol The name of the column containing the RSVP status code (default: "rsvpStatus").
 * @constructor Creates a new instance of [RsvpStatusTransformer] with an optional input column name and UID.
**/
class RsvpStatusTransformer(
    private val inputCol: String = "rsvpStatus",
    uid: String = Identifiable.randomUID("RsvpStatusTransformer")
) : BaseTransformer(uid) {

    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        val mappingExpr = `when`(col(inputCol).equalTo(RsvpStatus.UNANSWERED.code), RsvpStatus.UNANSWERED.name.lowercase())
            .`when`(col(inputCol).equalTo(RsvpStatus.ACCEPTED.code), RsvpStatus.ACCEPTED.name.lowercase())
            .`when`(col(inputCol).equalTo(RsvpStatus.DECLINED.code), RsvpStatus.DECLINED.name.lowercase())
            .otherwise(null)

        return dataset.withColumn(inputCol, mappingExpr)
    }
}
