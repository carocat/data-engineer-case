package com.spond.common.models

import java.sql.Timestamp

/**
 * Represents the root entity in the data model: a team.
 *
 * @property teamId Unique identifier for the team (Primary Key).
 * @property teamActivity Optional description of the team's activity.
 * @property countryCode Optional ISO country code where the team is based.
 * @property createdAt Timestamp when the team was created.
 * @property year Partition column derived from createdAt.
 * @property month Partition column derived from createdAt.
 * @property day Partition column derived from createdAt.
 */
data class Team(
    val teamId: String,
    val teamActivity: String?,
    val countryCode: String?,
    val createdAt: Timestamp,
    val year: Int,
    val month: Int,
    val day: Int
)

/**
 * Represents a member's affiliation to a team.
 *
 * @property membershipId Unique identifier for the membership (Primary Key).
 * @property teamId Foreign key referencing [Team.teamId].
 * @property roleTitle Optional title or role of the member within the team.
 * @property joinedAt Timestamp when the member joined the team.
 * @property year Partition column derived from joinedAt.
 * @property month Partition column derived from joinedAt.
 * @property day Partition column derived from joinedAt.
 */
data class Membership(
    val membershipId: String,
    val teamId: String,
    val roleTitle: String?,
    val joinedAt: Timestamp,
    val year: Int,
    val month: Int,
    val day: Int
)

/**
 * Represents an event hosted by a team.
 *
 * @property eventId Unique identifier for the event (Primary Key).
 * @property teamId Foreign key referencing [Team.teamId].
 * @property eventStart Optional timestamp when the event begins.
 * @property eventEnd Optional timestamp when the event ends.
 * @property latitude Optional latitude of the event's location.
 * @property longitude Optional longitude of the event's location.
 * @property createdAt Optional timestamp when the event was created.
 * @property year Partition column derived from createdAt.
 * @property month Partition column derived from createdAt.
 * @property day Partition column derived from createdAt.
 */
data class Event(
    val eventId: String,
    val teamId: String,
    val eventStart: Timestamp?,
    val eventEnd: Timestamp?,
    val latitude: Double?,
    val longitude: Double?,
    val createdAt: Timestamp?,
    val year: Int,
    val month: Int,
    val day: Int
)

/**
 * Represents a response to an event by a team member.
 *
 * @property eventRsvpId Unique identifier for the RSVP (Primary Key).
 * @property eventId Foreign key referencing [Event.eventId].
 * @property membershipId Foreign key referencing [Membership.membershipId].
 * @property rsvpStatus Optional numeric status (e.g., 1 for yes, 0 for no, etc.).
 * @property respondedAt Optional timestamp when the RSVP was recorded.
 * @property year Partition column derived from respondedAt.
 * @property month Partition column derived from respondedAt.
 * @property day Partition column derived from respondedAt.
 */
data class EventRsvp(
    val eventRsvpId: String,
    val eventId: String,
    val membershipId: String,
    val rsvpStatus: Int?,
    val respondedAt: Timestamp?,
    val year: Int,
    val month: Int,
    val day: Int
)
