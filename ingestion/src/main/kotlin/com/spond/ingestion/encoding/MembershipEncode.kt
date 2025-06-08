package com.spond.ingestion.encoding

import com.spond.common.utils.DatasetUtils.validateNonNulls
import com.spond.common.models.Membership
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row

object MembershipEncode {

    private val encoder = Encoders.bean(Membership::class.java)

    fun transform(dataset: Dataset<Row>): Dataset<Membership> {
        val validated = validateNonNulls(dataset, listOf("membership_id", "group_id", "joined_at"))

        return validated.map(
            MapFunction { row ->
                Membership(
                    membershipId = row.getAs("membership_id"),
                    teamId = row.getAs("group_id"),
                    roleTitle = row.getAs("role_title"),
                    joinedAt = row.getAs("joined_at"),
                    year = row.getAs("year"),
                    month = row.getAs("month"),
                    day = row.getAs("day")
                )
            },
            encoder
        )
    }
}
