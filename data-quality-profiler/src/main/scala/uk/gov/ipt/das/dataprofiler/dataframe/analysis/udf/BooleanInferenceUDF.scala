package uk.gov.ipt.das.dataprofiler.dataframe.analysis.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import TypeInferenceUDF.UNKNOWN_VALUE

object BooleanInferenceUDF extends UDF {
  // look in each path if its a boolean or not
  override def udf: String => String = {
    case "TRUE" => "Boolean"
    case "FALSE" => "Boolean"
    case "YES" => "Boolean"
    case "NO" => "Boolean"
    case "1" => "Boolean"
    case "0" => "Boolean"
    case _ => UNKNOWN_VALUE
  }

  override def name: String = "booleanUDF"
}
