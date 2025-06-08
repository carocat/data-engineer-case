import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object Schemas {
    val teamSchema = StructType(arrayOf(
        StructField("team_id", DataTypes.StringType, false, Metadata.empty()),
        StructField("team_activity", DataTypes.StringType, true, Metadata.empty()),
        StructField("country_code", DataTypes.StringType, false, Metadata.empty()),
        StructField("created_at", DataTypes.TimestampType, false, Metadata.empty())
    ))


    val membershipSchema = StructType(arrayOf(
        StructField("membership_id", DataTypes.StringType, false, Metadata.empty()),
        StructField("group_id", DataTypes.StringType, false, Metadata.empty()),  // was team_id
        StructField("role_title", DataTypes.StringType, true, Metadata.empty()),  // was user_id (different meaning)
        StructField("joined_at", DataTypes.TimestampType, true, Metadata.empty())
    ))

    val eventSchema = StructType(arrayOf(
        StructField("event_id", DataTypes.StringType, false, Metadata.empty()),
        StructField("team_id", DataTypes.StringType, false, Metadata.empty()),
        StructField("event_start", DataTypes.TimestampType, true, Metadata.empty()),  // renamed from start_time
        StructField("event_end", DataTypes.TimestampType, true, Metadata.empty()),    // renamed from end_time
        StructField("latitude", DataTypes.DoubleType, true, Metadata.empty()),
        StructField("longitude", DataTypes.DoubleType, true, Metadata.empty()),
        StructField("created_at", DataTypes.TimestampType, true, Metadata.empty())  // if needed
    ))

    val eventRsvpSchema = StructType(arrayOf(
        StructField("event_rsvp_id", DataTypes.StringType, false, Metadata.empty()),  // renamed from rsvp_id
        StructField("event_id", DataTypes.StringType, false, Metadata.empty()),
        StructField("membership_id", DataTypes.StringType, false, Metadata.empty()),  // replacing user_id with membership_id?
        StructField("rsvp_status", DataTypes.IntegerType, true, Metadata.empty()),     // renamed from response
        StructField("responded_at", DataTypes.TimestampType, true, Metadata.empty())
    ))

}
