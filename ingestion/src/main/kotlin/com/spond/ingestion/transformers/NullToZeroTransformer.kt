package com.spond.ingestion.transformers

import com.spond.common.transformers.BaseTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.lit

class NullToZeroTransformer(
    private val inputCol: String,
    private val outputCol: String = inputCol,
    uid: String = Identifiable.randomUID("NullToZeroTransformer")
) : BaseTransformer(uid) {

    override fun doTransform(dataset: Dataset<Row>): Dataset<Row> {
        return dataset.withColumn(outputCol, coalesce(col(inputCol), lit(0)))
    }
}
