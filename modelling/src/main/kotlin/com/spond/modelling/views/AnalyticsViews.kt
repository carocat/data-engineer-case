package com.spond.modelling.views

import com.spond.modelling.transformers.AcceptanceRateTransformer
import com.spond.modelling.transformers.DaysInRangeTransformer
import com.spond.modelling.transformers.MemberActivityClassifierTransformer
import com.spond.modelling.transformers.MemberHostOrUpdateEventTransformer
import com.spond.modelling.transformers.RsvpSummaryTransformer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

object AnalyticsViews {

    fun createAllViews(
        memberships: Dataset<Row>,
        events: Dataset<Row>,
        eventRsvps: Dataset<Row>
    ) {
        createDailyTeamActivityView(events)
        createRsvpSummaryPerEventDayView(eventRsvps)
        createEventAttendanceRateView(eventRsvps)
        createWeeklyMemberStatusView(memberships, eventRsvps)
        //TODO missing region view
    }

    fun createDailyTeamActivityView(
        events: Dataset<Row>,
        viewName: String = "vw_daily_team_activity"
    ) {
        val eventRsvpsTransformed = Pipeline().setStages(
            arrayOf(
                DaysInRangeTransformer("createdAt", "createdAt"),
                MemberHostOrUpdateEventTransformer()
            )
        ).fit(events).transform(events)
        eventRsvpsTransformed.createOrReplaceTempView(viewName)
    }

    fun createRsvpSummaryPerEventDayView(
        eventRsvps: Dataset<Row>,
        viewName: String = "vw_rsvp_summary_per_event_day"
    ) {
        RsvpSummaryTransformer().transform(eventRsvps).createOrReplaceTempView(viewName)
    }

    fun createEventAttendanceRateView(
        eventRsvps: Dataset<Row>,
        viewName: String = "vw_event_attendance_rate"
    ) {
        AcceptanceRateTransformer().transform(eventRsvps).createOrReplaceTempView(viewName)
    }

    fun createWeeklyMemberStatusView(
        memberships: Dataset<Row>,
        eventRsvp: Dataset<Row>,
        viewName: String = "vw_weekly_member_status"
    ) {
        MemberActivityClassifierTransformer(eventRsvp).transform(memberships).createOrReplaceTempView(viewName)
    }
}
