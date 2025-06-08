package com.spond.ingestion.encoding

import com.spond.common.utils.DatasetUtils.validateNonNulls
import com.spond.common.models.EventRsvp
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import java.sql.Timestamp
import java.time.ZoneId

object EventRsvpEncode {
    private val encoder = Encoders.bean(EventRsvp::class.java)

    fun transform(dataset: Dataset<Row>): Dataset<EventRsvp> {
        val validated = validateNonNulls(dataset, listOf("event_rsvp_id", "event_id", "membership_id"))

        return validated.map(
            MapFunction { row ->
                val respondedAt = row.getAs<Timestamp?>("responded_at")
                val zonedDateTime = respondedAt?.toInstant()?.atZone(ZoneId.of("UTC"))

                EventRsvp(
                    eventRsvpId = row.getAs("event_rsvp_id"),
                    eventId = row.getAs("event_id"),
                    membershipId = row.getAs("membership_id"),
                    rsvpStatus = row.getAs<Int?>("rsvp_status"),
                    respondedAt = respondedAt,
                    year = zonedDateTime?.year ?: 1970,
                    month = zonedDateTime?.monthValue ?: 1,
                    day = zonedDateTime?.dayOfMonth ?: 1
                )
            },
            encoder
        )
    }
}
