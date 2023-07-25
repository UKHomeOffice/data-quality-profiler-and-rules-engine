package uk.gov.ipt.das.dataprofiler.dataframe.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRowEncoder._
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.ProfiledDataFrame

class MetricsPercentDataFrame private (dataFrameIn: ProfiledDataFrame,
                                       lowerThreshold: Int,
                                       upperThreshold: Int) extends VersionedDataFrame {
  val ROW_NUMBER: String = "rowNumber"
  val MAX_ROW_NUMBER: String = "maxRowNumber"
  val PERCENT_OCCURRENCE: String = "percentOccurrence"
  val SUM: String = "sum"
  val ORIGINAL_VALUE: String = "originalValue"
  val SAMPLE_VALUE: String = "sampleValue"
  val RECORD_ID: String = "recordId"

  private lazy val dfDropColumn =
    dataFrameIn.dataFrame.map{ df =>
      // windows functions
      val windowRowRank = Window
        .partitionBy(FEATURE_PATH, FEATURE_VALUE, FEATURE_NAME)
        .orderBy(col(FEATURE_VALUE))
      val windowRowSum = Window.partitionBy(FEATURE_PATH, FEATURE_NAME)

      // Do SQL windowing
      val windowedDf = df.withColumn(ROW_NUMBER, row_number.over(windowRowRank))
      val maxRowDf = windowedDf
        .withColumn(MAX_ROW_NUMBER, max(ROW_NUMBER).over(windowRowRank))
        .filter(col(MAX_ROW_NUMBER) === col(ROW_NUMBER))
      val sumRowDf = maxRowDf.withColumn(SUM, sum(col(MAX_ROW_NUMBER)).over(windowRowSum))

      // calculate % column
      val dfPercent = sumRowDf
        .withColumn(PERCENT_OCCURRENCE,
          lit(100) * (col(MAX_ROW_NUMBER) / col(SUM)))

      val dfRounded = dfPercent
        .withColumn(PERCENT_OCCURRENCE, round(col(PERCENT_OCCURRENCE), 2))

      // Rename and drop unwanted columns
      dfRounded
        .withColumnRenamed(MAX_ROW_NUMBER, COUNT)
        .withColumnRenamed(ORIGINAL_VALUE,SAMPLE_VALUE)
        .drop(SUM, RECORD_ID, ROW_NUMBER)
    }

  override lazy val dataFrame: Option[DataFrame] = dfDropColumn
  override val schemaVersion: String = "schemaV1"
  override val name: String = "profiler-masks-with-aggregate-counts-percentages-and-samples"

  lazy val dataFrameWithThresholds: Option[DataFrame] =
    dfDropColumn.map { df =>
      df.filter(
        col(PERCENT_OCCURRENCE) >= lowerThreshold && col(PERCENT_OCCURRENCE) <= upperThreshold
      )
    }

}
object MetricsPercentDataFrame {
  def apply(dataFrame: ProfiledDataFrame, lowerThreshold: Int, upperThreshold: Int): MetricsPercentDataFrame =
    new MetricsPercentDataFrame(
      dataFrameIn = dataFrame,
      lowerThreshold = lowerThreshold,
      upperThreshold = upperThreshold
    )
}
