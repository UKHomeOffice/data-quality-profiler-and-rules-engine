package uk.gov.ipt.das.dataprofiler.dataframe.analysis.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

object TypeInferenceUDF extends UDF {

  lazy val UNKNOWN_VALUE = "_UNKNOWN"

  // look in each path if its an integer, date, time, datetime or not
  override def udf: String => String = {
    case "9" => "integer"
    case "9-9-9" => "date"
    case "9/9/9" => "date"
    case "9-9-9A9:9" => "dateTime"
    case "9:9" => "time"
    case _ => UNKNOWN_VALUE
  }

  override def name: String = "typeUDF"
}
