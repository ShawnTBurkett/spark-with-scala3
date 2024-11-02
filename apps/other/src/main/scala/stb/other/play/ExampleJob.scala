package stb.other.play

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.{UDF1, UDF2}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import stb.core.play.Foo
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import scala.reflect.runtime.universe

object ExampleJob {

  // only the driver should know this
  Foo.setNonDefaultValue()

  /**
   * [[SparkSession]] object for this job. Note that the spark master must be available at
   * `spark://127.0.0.1:7077` or this will not work.
   */
  val spark: SparkSession = {
    SparkSession
      .builder()
      .master("spark://127.0.0.1:7077")
      .config(
        Map(
          "spark.executor.memory" -> "1g",
          "spark.num.executors" -> 3,
          "spark.executor.cores" -> 2
        )
      )
      .getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR")

  /**
   * Creates [[DataFrame]] object to test the spark cluster.
   * @return
   */
  def testDF: DataFrame = {
    val rdd: RDD[Row] = spark.sparkContext.parallelize((0 to 10).map(i => Row(i, Foo.foo)), 4)
    val schema: StructType = {
      StructType(StructField("row", IntegerType, true) :: StructField("driver_value", StringType, true) :: Nil)
    }
    spark.sqlContext.createDataFrame(rdd, schema)
  }

  /**
   * User defined function that forces each executor to initialize the object [[Foo]].
   * @return [[UserDefinedFunction]] object
   *
   * @note Unfortunately scala type tags are no longer supported, so the basic scala types must be cast as java
   *       types.
   */
  def testUdf: UserDefinedFunction = {
    import java.lang.{Integer => JInteger, String => JString}
    udf (
      new UDF1[JInteger, JString] {
        def call(anything: JInteger): JString = Foo.foo
      },
      DataTypes.StringType
    )
  }

  def main(args: Array[String]): Unit = {
    println(testDF.withColumn("executor_value", testUdf.apply(col("row"))).show())
  }

}

