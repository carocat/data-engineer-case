package com.spond.modelling.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.`when`
import org.apache.spark.sql.functions.countDistinct

//Daily active teams: How many distinct teams hosted or updated events each day?
class MemberHostOrUpdateEventTransformer (
    uid: String = Identifiable.randomUID("MemberHostOrUpdateEventTransformer")
) : BaseTransformer(uid) {
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        val ds = dataset.withColumn(
            "isActive",
            `when`(
                // Check if the given day is between the event's start and end timestamps
                col("date").between(col("eventStart"), col("eventEnd"))
                    // OR if the day matches the event creation date (i.e., it was updated on this day)
                    .or(col("date").equalTo(to_date(col("createdAt")))),
                true
            ).otherwise(false) // Mark as not active if neither condition is met
        )

        return ds
            .filter(col("isActive")) // Only keep rows where the team was active
            .select("teamId", "date_year", "date_month", "date_day") // Retain necessary columns
            .distinct() // Prevent duplicate (teamId, day) entries
            .groupBy("date_year", "date_month", "date_day") // Group by full calendar breakdown
            .agg(countDistinct("teamId").alias("teamsActive"))
            .select(col("teamsActive"),
                col("date_year").`as`("year"),
                col("date_month").`as`("mont"),
                col("date_day").`as`("day")
            )
    }
}

