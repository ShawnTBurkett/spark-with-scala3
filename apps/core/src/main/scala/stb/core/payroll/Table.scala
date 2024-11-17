package stb.core.payroll

import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.functions.{col, countDistinct, struct}
import org.apache.spark.sql.types.DecimalType

import java.security.InvalidParameterException
import java.time.LocalDate
import scala.annotation.StaticAnnotation
import scala.annotation.meta.{field, param}
import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.runtime.universe as ru

abstract class Table[T <: TableSchema : ru.TypeTag] {
  val name: String = toSnakeCase(ru.typeTag[T].tpe.toString)
  private val _data: mutable.ArrayBuffer[T] = mutable.ArrayBuffer[T]()
  private implicit val encoder: Encoder[T] = Encoders.product[T]
  def data: Seq[T] = _data.toSeq
  def addRows(rows: T*): Unit = _data.appendAll(rows)
  def removeRow(row: T): Unit = {
    lazy val index: Int = _data.indexOf(row)
    if (index >= 0) _data.remove(_data.indexOf(row))
  }
  def asDataFrame(implicit spark: SparkSession): DataFrame = {
    import spark.implicits.*
    lazy val df: DataFrame = data.toDF()
    data.head.primaryKey match {
      case Some(col) => if (df.select(countDistinct(col)).head.getLong(0) != df.count()) {
        throw new RuntimeException("Primary key is not unique; cannot create data frame.")
      }
      case None => ()
    }
    data.head.decimalTypeCols.foldLeft(df){ case (inter, (c, n)) =>
      inter.withColumn(c, col(c).cast(DecimalType(n.precision, n.scale)))
    }
  }
  def save(implicit spark: SparkSession): Unit = {
    asDataFrame.coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(s"$name.csv")
  }
}

object Table {
  def createTable[T <: TableSchema : ru.TypeTag]: Table[T] = new Table[T]{}
  def loadTableFromCsv[T <: TableSchema : ru.TypeTag](path: String
                                                     )(implicit spark: SparkSession): Table[T] = {
    lazy val rows: Array[T] = {
      spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv(path)
        .as[T](Encoders.product[T])
        .collect()
    }

    val table: Table[T] = createTable
    table.addRows(rows: _*)
    table
  }
}
