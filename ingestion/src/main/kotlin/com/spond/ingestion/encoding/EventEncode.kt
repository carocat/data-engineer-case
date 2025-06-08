package com.spond.ingestion.encoding

import com.spond.common.utils.DatasetUtils.validateNonNulls
import com.spond.common.models.Event
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.functions.col
import java.sql.Timestamp
import java.time.ZoneId

object EventEncode {

    private val encoder = Encoders.bean(Event::class.java)

    fun transform(dataset: Dataset<Row>): Dataset<Event> {
        val validated = validateNonNulls(dataset, listOf("event_id", "team_id", "created_at"))

        val dfWithDoubles = validated
            .withColumn("latitude", col("latitude").cast("double"))
            .withColumn("longitude", col("longitude").cast("double"))

        return dfWithDoubles.map(
            MapFunction { row ->
                val createdAt = row.getAs<Timestamp>("created_at")
                val zonedDateTime = createdAt?.toInstant()?.atZone(ZoneId.of("UTC"))

                Event(
                    eventId = row.getAs("event_id"),
                    teamId = row.getAs("team_id"),
                    eventStart = row.getAs<Timestamp>("event_start"),
                    eventEnd = row.getAs<Timestamp>("event_end"),
                    latitude = row.getAs<Double?>("latitude"),
                    longitude = row.getAs<Double?>("longitude"),
                    createdAt = createdAt,
                    year = zonedDateTime?.year ?: 1970,
                    month = zonedDateTime?.monthValue ?: 1,
                    day = zonedDateTime?.dayOfMonth ?: 1
                )
            },
            encoder
        )
    }
}
