import com.spond.ingestion.transformers.NullToZeroTransformer
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NullToZeroTransformerTest {

    private val spark = SparkSession.builder()
        .appName("Test")
        .master("local[*]")
        .getOrCreate()

    @Test
    fun testNullToZero() {
        val schema = StructType(
            arrayOf(
                StructField("value", DataTypes.IntegerType, true, Metadata.empty())
            )
        )

        val data = listOf(
            RowFactory.create(null as Int?),
            RowFactory.create(5)
        )
        val df = spark.createDataFrame(data, schema)

        val transformer = NullToZeroTransformer("value", "value_no_nulls")

        val result = transformer.transform(df)
        result.show()
        val collected = result.select("value_no_nulls").collectAsList()

        assertEquals(0, collected[0].getInt(0))  // null replaced with 0
        assertEquals(5, collected[1].getInt(0))
    }
}
