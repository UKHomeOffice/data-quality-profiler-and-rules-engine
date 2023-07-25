package uk.gov.ipt.das.dataprofiler.dataframe.analysis.udf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

trait UDF {
  def name: String
  def udf: String => String
  def register(implicit spark: SparkSession): UserDefinedFunction = spark.udf.register(name, udf)
}
