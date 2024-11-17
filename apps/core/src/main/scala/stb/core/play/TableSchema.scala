package stb.core.play

import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}
import scala.language.implicitConversions
import java.time.LocalDate

import scala.annotation.StaticAnnotation
final class numeric(val precision: Int, val scale: Int) extends StaticAnnotation

import scala.annotation.meta.{field, param}


def uuid: String = java.util.UUID.randomUUID.toString
def toSnakeCase(str: String): String = {
  str.zipWithIndex.map{
    case (s: Char, idx: Int) => if (s.toUpper == s && idx > 0) s"_${s.toLower}" else s.toLower
  }.mkString
}

implicit val spark: SparkSession = ???
import spark.implicits._

// define schemas
sealed trait TableSchema extends Product with Serializable {
  val name: String = toSnakeCase(this.getClass.getSimpleName)

  case class Numeric(precision: Int, scale: Int)

  implicit def toNumeric(t: (Int, Int)): Numeric = (Numeric.apply _).tupled(t)

  val mirror = ru.runtimeMirror(this.getClass.getClassLoader)
  val im = mirror.reflect(this)
  val decimalTypeCols = {
    im.symbol.primaryConstructor.typeSignature.paramLists.head
      .collect {
        case s if s.annotations.nonEmpty => (s.name, s.annotations.head.tree.children.tail.map(_.toString.toInt))
      }
      .map { case (s, l) => (s.toString, Numeric(l.head, l(1))) }.toMap
  }

}

trait Table[T <: TableSchema : Encoder] {
  val tableSchema: T

  var _data: mutable.ArrayBuffer[T]
  def data: Seq[T] = _data.toSeq
  def addRows(row: T): Unit = _data.append(row)
  def addRows(rows: Seq[T]) = _data.appendAll(rows)
  def asDataFrame(): DataFrame = {
    lazy val df: DataFrame = data.toDF()
    tableSchema.decimalTypeCols.foldLeft(df){ case (inter, (c, n)) =>
      inter.withColumn(c, col(c).cast(DecimalType(n.precision, n.scale)))
    }
  }
  def save(): Unit = {
    asDataFrame().coalesce(1)
      .write.format("csv")
      .option("header", "true")
      .save(s"${tableSchema.name}.csv")
  }
}

case class TimeCardRecords(date: LocalDate,
                           date_entered: LocalDate,
                           pay_day: LocalDate,
                           off_cycle: Boolean,
                           @numeric(12, 2) labor: Double,
                           cost_center: String,
                           po: String,
                           program: String,
                           invoice_number: String,
                           consultant_id: String,
                           consultant_name: String,
                           description: String,
                           row_hash: String = uuid
                          ) extends TableSchema

case class  ExpenseRecords(off_cycle: Boolean,
                           date_entered: LocalDate,
                           pay_day: LocalDate,
                           date: LocalDate,
                           @numeric(12, 2) cost: Double,
                           po: String,
                           consultant_name: String,
                           invoice_number: String,
                           consultant_id: String,
                           program: String,
                           description: String,
                           row_hash: String = uuid) extends TableSchema

case class CreditRecords(off_cycle: Boolean,
                         date_entered: LocalDate,
                         pay_day: LocalDate,
                         date: LocalDate,
                         @numeric(12, 2) cost: Double,
                         po: String,
                         consultant_name: String,
                         invoice_number: String,
                         consultant_id: String,
                         program: String,
                         description: String,
                         row_hash: String = uuid) extends TableSchema

case class BillingRates(consultant_id: String,
                        client: String,
                        @numeric(12,2) rate_to_client: Double,
                        @numeric(12,2) rate_to_sri: Double,
                        effective_date: LocalDate,
                        @numeric(12,2) overhead: Double,
                        inactive: Boolean) extends TableSchema

case class ConsultantCostCenter(program: String,
                                consultant_id: String,
                                cost_center: String,
                                title: String) extends TableSchema
case class ClientsPrograms(client: String,
                           program: String,
                           invoice_prefix: String,
                           po: String,
                           effective_date: LocalDate,
                           address_line_1: String,
                           address_line_2: String,
                           address_line_3: String,
                           address_line_4: String,
                           address_line_5: String,
                           address_line_6: String,
                           address_line_7: String) extends TableSchema

case class PoBalance(po: String,
                     program: String,
                     @numeric(12,2)
                     initial_funding_amount: Double,
                     date: LocalDate, @numeric(12,2)
                     balance: Double) extends TableSchema

case class UitrTable(
                      effective_year: String,
                      @numeric(12,5) annual_threshold: Double,
                      @numeric(12,5) annual_rate: Double) extends TableSchema

case class Distributions(date: LocalDate,
                         consultant_id: String,
                         @numeric(12,2) distribution_amount: Double) extends TableSchema

case class HealthInsuranceAndBonus(pay_date: LocalDate,
                                   consultant_id: String,
                                   @numeric(12,2) bonus: Double,
                                   @numeric(12,2) health_insurance: Double,
                                   @numeric(12,2) hsa: Double) extends TableSchema

case class FicaMedicareTaxTables(
                                  pay_date: LocalDate,
                                  @numeric(12,3) ee_fica: Double,
                                  @numeric(12,3) ee_medicare: Double,
                                  @numeric(12,3) er_fica: Double,
                                  @numeric(12,3) er_medicare: Double) extends TableSchema

case class InvoiceSummary (@numeric(12,2) invoice_total: Double,
                           @numeric(12,2) expense_total: Double,
                           @numeric(12,2) credit_total: Double,
                           client: String,
                           program: String,
                           po: String,
                           invoice_number: String,
                           date_billed: LocalDate) extends TableSchema

case class FedAndStateWithholding(pay_date: LocalDate,
                                  consultant_id: String,
                                  @numeric(12,4) fed_tax_percent: Double,
                                  @numeric(12,4) fed_tax_amount: Double,
                                  @numeric(12,4) state_tax_percent: Double,
                                  @numeric(12,4) state_tax_amount: Double) extends TableSchema

case class Consultants(consultant_id: String,
                       is_employee: Boolean,
                       is_inactive: Boolean,
                       first_name: String,
                       last_name: String,
                       short_name: String,
                       long_name: String,
                       company_name: String,
                       ssn: String,
                       ein: String,
                       address1: String,
                       address2: String,
                       city: String,
                       state: String,
                       zip: String,
                       phone: String,
                       email: String) extends TableSchema

// primary key(consultant_id, quarter, year)
case class QuarterlyReports(
                             consultant_id: String,
                             report_date: LocalDate,
                             quarter: Int,
                             year: Int,
                             @numeric(12,2) gross: Double,
                             @numeric(12,2) taxable: Double,
                             @numeric(12,2) federal_deposit: Double,
                             @numeric(12,2) uitr: Double) extends TableSchema

// primary key(consultant_id, pay_day)
case class MonthlyPayrollRecords(consultant_id: String,
                                 pay_day: LocalDate,
                                 @numeric(12,2) labor: Double,
                                 @numeric(12,2) regular: Double,
                                 @numeric(12,2) bonus: Double,
                                 @numeric(12,2) hsa: Double,
                                 @numeric(12,2) health_insurance: Double,
                                 @numeric(12,2) gross: Double,
                                 @numeric(12,2) taxable: Double,
                                 @numeric(12,2) ee_fica: Double,
                                 @numeric(12,2) ee_medicare: Double,
                                 @numeric(12,2) fed_wh: Double,
                                 @numeric(12,2) state_wh: Double,
                                 @numeric(12,2) net_pay: Double,
                                 @numeric(12,2) er_fica: Double,
                                 @numeric(12,2) er_medicare: Double,
                                 @numeric(12,2) total_federal_tax_deposit: Double) extends TableSchema

// primary key(consultant_id, invoice_number)
case class ConsultantPayRecords(consultant_id: String,
                                consultant_name: String,
                                pay_day: LocalDate,
                                invoice_number: String,
                                @numeric(12,2) labor: Double,
                                program: String,
                                @numeric(12,2) rate_to_sri: Double,
                                @numeric(12,2) total: Double,
                                @numeric(12,2) expenses: Double,
                                @numeric(12,2) total_plus_expenses: Double,
                                date_paid: LocalDate,
                                check_number: String) extends TableSchema

// date primary key
case class MonthlySummaries(pay_day: LocalDate,
                            @numeric(12,2) total_federal_tax_deposit: Double,
                            @numeric(12,2) state_wh: Double,
                            date_paid: LocalDate,
                            check_number: String) extends TableSchema

// date primary key
case class QuarterlySummaries(date: LocalDate,
                              @numeric(12,2) uitr: Double,
                              date_paid: LocalDate,
                              check_numbe: String)extends TableSchema
