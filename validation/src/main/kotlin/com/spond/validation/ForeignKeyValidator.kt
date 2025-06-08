package com.spond.validation

import com.spond.common.models.Team
import com.spond.common.models.Membership
import com.spond.common.models.Event
import com.spond.common.models.EventRsvp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders

/**
 * Performs referential integrity checks between datasets,
 * assuming the following dependency hierarchy:
 *
 * Team (root)
 * ├── Membership
 * │   └── Event
 * │       └── EventRsvp
 *
 * All functions return records that violate the integrity constraint.
 */
object ForeignKeyValidator {

    private val membershipEncoder = Encoders.bean(Membership::class.java)
    private val eventEncoder = Encoders.bean(Event::class.java)
    private val eventRsvpEncoder = Encoders.bean(EventRsvp::class.java)

    /**
     * Finds all Membership records whose `teamId` does not exist in the Teams dataset.
     *
     * This validates that every membership is associated with a valid team.
     *
     * @return Dataset of orphaned memberships (violating foreign key to teams)
     */
    fun membershipsWithoutTeams(
        memberships: Dataset<Membership>,
        teams: Dataset<Team>
    ): Dataset<Membership> {
        return memberships.join(
            teams,
            memberships.col("teamId").equalTo(teams.col("teamId")),
            "left_anti"
        ).`as`(membershipEncoder)
    }

    /**
     * Finds all Event records whose `teamId` does not exist in the Memberships dataset.
     *
     * This validates that every event is tied to a valid membership.
     * Adjust this if events are linked directly to teams instead of memberships.
     *
     * @return Dataset of orphaned events (violating foreign key to memberships)
     */
    fun eventsWithoutMemberships(
        events: Dataset<Event>,
        memberships: Dataset<Membership>
    ): Dataset<Event> {
        return events.join(
            memberships,
            events.col("teamId").equalTo(memberships.col("teamId")),
            "left_anti"
        ).`as`(eventEncoder)
    }

    /**
     * Finds all EventRsvp records whose `eventId` does not exist in the Events dataset.
     *
     * This ensures all RSVPs are associated with a valid event.
     *
     * @return Dataset of orphaned RSVPs (violating foreign key to events)
     */
    fun eventRsvpsWithoutEvents(
        eventRsvps: Dataset<EventRsvp>,
        events: Dataset<Event>
    ): Dataset<EventRsvp> {
        return eventRsvps.join(
            events,
            eventRsvps.col("eventId").equalTo(events.col("eventId")),
            "left_anti"
        ).`as`(eventRsvpEncoder)
    }

    /**
     * Finds all EventRsvp records whose `membershipId` does not exist in the Memberships dataset.
     *
     * This ensures that every RSVP is linked to a valid user (via membership).
     *
     * @return Dataset of orphaned RSVPs (violating foreign key to memberships)
     */
    fun eventRsvpsWithoutMemberships(
        eventRsvps: Dataset<EventRsvp>,
        memberships: Dataset<Membership>
    ): Dataset<EventRsvp> {
        return eventRsvps.join(
            memberships,
            eventRsvps.col("membershipId").equalTo(memberships.col("membershipId")),
            "left_anti"
        ).`as`(eventRsvpEncoder)
    }
}
