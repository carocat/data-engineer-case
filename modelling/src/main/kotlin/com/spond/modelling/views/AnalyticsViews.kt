package com.spond.modelling.views

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.to_date

// Work in progress: placeholder views will be expanded as needed
object AnalyticsViews {

    fun createAllViews(
        teams: Dataset<Row>,
        memberships: Dataset<Row>,
        events: Dataset<Row>,
        eventRsvps: Dataset<Row>
    ) {
        println("createAllViews invoked with:")
        println("- teams: ${teams.count()} rows")

        createDailyTeamActivityView(events)
        createRsvpSummaryPerEventDayView(eventRsvps)
        createEventAttendanceRateView(eventRsvps)
        createWeeklyMemberStatusView(memberships)
    }

    fun createDailyTeamActivityView(
        events: Dataset<Row>,
        createdAtCol: String = "createdAt",
        teamIdCol: String = "teamId",
        eventIdCol: String = "eventId",
        viewName: String = "vw_daily_team_activity"
    ) {
        val viewDf = events
            .withColumn("eventDay", to_date(col(createdAtCol)))
            .groupBy(col("eventDay"), col(teamIdCol))
            .agg(countDistinct(col(eventIdCol)).alias("eventsCreated"))

        viewDf.createOrReplaceTempView(viewName)
    }

    fun createRsvpSummaryPerEventDayView(eventRsvps: Dataset<Row>) {
        // TODO: implement logic
        println("createRsvpSummaryPerEventDayView placeholder received ${eventRsvps.count()} rows")
    }

    fun createEventAttendanceRateView(eventRsvps: Dataset<Row>) {
        // TODO: implement logic
        println("createEventAttendanceRateView placeholder received ${eventRsvps.count()} rows")
    }

    fun createWeeklyMemberStatusView(memberships: Dataset<Row>) {
        // TODO: implement logic
        println("createWeeklyMemberStatusView placeholder received ${memberships.count()} rows")
    }
}
