package stb.core.payroll

import scala.annotation.StaticAnnotation

object utils {
  /**
   * Annotation to determine which case class parameters should have 
   * [[org.apache.spark.sql.types.DecimalType decimal type]].
   * @param precision 
   *    maximum number of digits
   * @param scale
   *    maximum number of digits after decimal place
   */
  final class numeric(val precision: Int, val scale: Int) extends StaticAnnotation

  /**
   * Generated UUID.
   * @return String
   */
  def uuid: String = java.util.UUID.randomUUID.toString

  /**
   * Converts Pascal or camel case to snake case. 
   * @param str 
   *    `String` in Pascal or camel case
   * @return String in snake case.
   */
  def toSnakeCase(str: String): String = {
    str.zipWithIndex.map {
      case (s: Char, idx: Int) => if (s.toUpper == s && idx > 0) s"_${s.toLower}" else s.toLower
    }.mkString
  }

}
