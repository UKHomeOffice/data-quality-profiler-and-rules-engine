package uk.gov.ipt.das.dataprofiler.dataframe.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRowEncoder._
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame
@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Any"))
class MetricsFileOutputDataFrame private (dataFrameIn: MetricsDataFrame) extends VersionedDataFrame {

  override lazy val dataFrame: Option[DataFrame] = dataFrameIn.dataFrame
  override val schemaVersion: String = "schemaV2"
  override val name: String = "profiler-masks-with-aggregate-counts-for-validity-assessments"

  val IS_VALID: String = "isValid"

   /**
   * Get the profiled dataframe and filter to the specified DQ type
   *
   * @return individual Excels for each field
   */
  def writeExcelPerField(filePath: String, DQType: String = "DQ_HIGHGRAIN"): Unit =
    dataFrameIn.dataFrame.fold {
      println(s"Could not writeExcelPerField - no data for $filePath with DQType $DQType")
    } { df =>
      val dfDQType = df.filter(col(FEATURE_NAME) === DQType)
      val dfNewCol = dfDQType.withColumn(IS_VALID, lit(null: String))
      val distinctValues = dfDQType.select(FEATURE_PATH).distinct().rdd.map(r => r(0)).collect().par

      distinctValues.foreach { field =>
        val df_filtered = dfNewCol.filter(col(FEATURE_PATH) === field.toString).sort(col(COUNT).desc)
        val fileName = s"$filePath/$field-$DQType.xlsx"
        df_filtered
          .repartition(1)
          .write
          .format("com.crealytics.spark.excel")
          .option("dataAddress", "A1")
          .option("header", "true")
          .mode("overwrite")
          .save(fileName)
      }
    }

  /**
   * Get the profiled dataframe and filter to the specified DQ type
   *
   * @return individual tsvs with a csv file extension for each field
   */
  def writeTSVPerField(filePath: String, DQType: String = "DQ_HIGHGRAIN"): Unit =
    dataFrameIn.dataFrame.fold {
      println(s"Could not writeTSVPerField - no data for $filePath with DQType $DQType")
    } { df =>
      val dfDQType = df.filter(col(FEATURE_NAME) === DQType)
      val dfNewCol = dfDQType.withColumn(IS_VALID, lit("TBC": String)).sort(col(COUNT).desc)
      dfNewCol
        .repartition(col(FEATURE_PATH))
        .write
        .option("delimiter", "\t")
        .option("header", "true")
        .partitionBy(FEATURE_PATH)
        .mode("overwrite")
        .csv(s"$filePath/$DQType/")
    }

}
object MetricsFileOutputDataFrame {
  def apply(dataFrame: MetricsDataFrame): MetricsFileOutputDataFrame =
    new MetricsFileOutputDataFrame(dataFrame)
}