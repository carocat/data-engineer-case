package com.spond.common.transformers

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Abstract base class for custom Spark ML Transformers.
 *
 * This class provides a foundation for building reusable transformations
 * in Spark pipelines by encapsulating the `transform` method logic and
 * enforcing a custom `doTransform` implementation.
 *
 * @param _uid Unique identifier for the transformer instance, used internally by Spark ML.
 */
abstract class BaseTransformer(
    private val _uid: String = Identifiable.randomUID("BaseTransformer"),
) : Transformer() {

    override fun uid(): String = _uid

    /**
     * Implement this method in subclasses to define the custom transformation logic.
     *
     * @param dataset The input Dataset of rows to transform.
     * @return The transformed Dataset.
     */
    abstract fun doTransform(dataset: Dataset<Row>): Dataset<Row>

    /**
     * Overrides the Spark ML `transform` method, forwarding to `doTransform`.
     */
    @Suppress("UNCHECKED_CAST")
    override fun transform(dataset: Dataset<*>): Dataset<Row> = doTransform(dataset as Dataset<Row>)

    /**
     * Copying is not supported for this transformer.
     */
    override fun copy(extra: ParamMap): Transformer =
        throw UnsupportedOperationException("Copy not implemented")

    /**
     * Returns the input schema unchanged. Override if schema transformation is needed.
     */
    override fun transformSchema(schema: StructType): StructType = schema
}

