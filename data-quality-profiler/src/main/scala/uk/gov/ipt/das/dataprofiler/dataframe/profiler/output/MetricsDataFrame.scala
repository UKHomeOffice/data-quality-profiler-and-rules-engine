package uk.gov.ipt.das.dataprofiler.dataframe.profiler.output

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRowEncoder._
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.analysis.SampleInjectedGrouping
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRow
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough

class MetricsDataFrame private (dataFrameIn: Option[DataFrame]) extends VersionedDataFrame {

  // filter the original value from the dataframe as not required for the aggregate outputs
  private lazy val filteredDataFrame = dataFrameIn.map {
    _.filter(col(FEATURE_NAME) =!= OriginalValuePassthrough.definition.getFieldName)
  }

  override lazy val dataFrame: Option[DataFrame] = filteredDataFrame
  override val schemaVersion: String = "schemaV2"
  override val name: String = "profiler-masks-with-aggregate-counts-and-samples"

  val ROW_NUMBER: String = "ROW_NUMBER"

  def truncate(recordLimit: Int, spark: SparkSession): (Option[DataFrame], Option[DataFrame]) = this.dataFrame match {
    case Some(dataFrame: DataFrame) =>
      val truncatedDf = dataFrame
        .withColumn(ROW_NUMBER,
          row_number.over(Window.partitionBy(FEATURE_NAME, FEATURE_PATH).orderBy(col(COUNT).desc)))
        .filter(col(ROW_NUMBER) <= recordLimit)
        .drop(ROW_NUMBER)

      val summary =
        truncatedDf
          .unionAll(dataFrame)
          .except(dataFrame.intersect(truncatedDf))
          .select(FEATURE_PATH, FEATURE_NAME, FEATURE_VALUE)
          .groupBy(FEATURE_PATH, FEATURE_NAME)
          .count()

      val trailerWarning = spark.createDataFrame(
        Seq(
          ("***", "this dataset has been truncated", "***", "***", "***", "***", "***", "***")
        )
      ).toDF(FEATURE_PATH, FEATURE_NAME, FEATURE_VALUE, SAMPLE_MIN, SAMPLE_MAX, SAMPLE_FIRST, SAMPLE_LAST, COUNT)


      (Option(truncatedDf.union(trailerWarning)), Option(summary))

    case None => (None, None)
  }
}
object MetricsDataFrame {
  def apply(additionalGroupByColumns: Seq[String], frrs: Dataset[FeatureReportRow]): MetricsDataFrame =
    new MetricsDataFrame(toDataFrame(additionalGroupByColumns, frrs))

  def apply(dataFrame: Option[DataFrame]): MetricsDataFrame =
    new MetricsDataFrame(dataFrameIn = dataFrame)

  def apply(additionalGroupByColumns: Seq[String], dataFrame: Option[DataFrame]): MetricsDataFrame =
    new MetricsDataFrame(dataFrameIn =
      Option(SampleInjectedGrouping.metricsDataFrameWithSamples(dataFrame, additionalGroupByColumns)))
}