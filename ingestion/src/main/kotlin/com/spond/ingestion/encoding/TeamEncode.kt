package com.spond.ingestion.encoding

import com.spond.common.utils.DatasetUtils.validateNonNulls
import com.spond.common.models.Team
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*

object TeamEncode {

    private val encoder = Encoders.bean(Team::class.java)

    fun transform(dataset: Dataset<Row>): Dataset<Team> {
        dataset.where(col("created_at").isNull).show()
        val validated = validateNonNulls(dataset, listOf("team_id", "country_code", "created_at"))


        val withPartitionCols = validated
            .withColumn("year", year(col("created_at")))
            .withColumn("month", month(col("created_at")))
            .withColumn("day", dayofmonth(col("created_at")))

        return withPartitionCols.map(
            MapFunction { row ->
                Team(
                    teamId = row.getAs("team_id"),
                    teamActivity = row.getAs("team_activity"),
                    countryCode = row.getAs("country_code"),
                    createdAt = row.getAs("created_at"),
                    year = row.getAs("year"),
                    month = row.getAs("month"),
                    day = row.getAs("day")
                )
            },
            encoder
        )
    }
}
