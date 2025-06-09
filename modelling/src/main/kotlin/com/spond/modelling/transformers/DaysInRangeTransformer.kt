package com.spond.modelling.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.sequence
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.dayofmonth

// In order to create views I need to know which date range I should use.
// NOTE: This logic is specific to the interview setup.
// In a production environment, this kind of daily aggregation would typically be handled
// by a scheduled job (e.g., a daily ETL pipeline).
// Backfilling would be handled separately through a dedicated backfill job
// using historic event data.
class DaysInRangeTransformer (
    private val dayMinRange: String,
    private val dayMaxRange: String,
    uid: String = Identifiable.randomUID("MemberHostOrUpdateEventTransformer")
) : BaseTransformer(uid) {
    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {

        val dateBounds = dataset.agg(
            min(to_date(col(dayMinRange))).alias("minDate"),
            max(to_date(col(dayMaxRange))).alias("maxDate")
        )

        val calendarDays = dateBounds
            .withColumn("day_array", sequence(col("minDate"), col("maxDate")))
            .withColumn("date", explode(col("day_array")))
            .select("date")
            .withColumn("date_year", year(col("date")))
            .withColumn("date_month", month(col("date")))
            .withColumn("date_day", dayofmonth(col("date")))

        return calendarDays.crossJoin(dataset)

    }
}