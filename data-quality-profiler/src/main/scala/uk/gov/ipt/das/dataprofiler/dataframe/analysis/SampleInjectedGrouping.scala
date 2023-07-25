package uk.gov.ipt.das.dataprofiler.dataframe.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollectionEncoder._
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRowEncoder.{FEATURE_NAME => _, FEATURE_PATH => _, FEATURE_VALUE => _, _}
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
object SampleInjectedGrouping {

  def metricsDataFrameWithSamples(dataFrame: Option[DataFrame],
                                  additionalGroupByColumns: Seq[String]): DataFrame = {
    val groupByColumns =
      Seq(FEATURE_PATH, FEATURE_NAME) ++
      additionalGroupByColumns ++
      Seq(FEATURE_VALUE)

    // N.B get can be used as this is only done when data is read back in from parquet
    //TODO need find default dataFrame or None case
    dataFrame
      .get
      .groupBy(groupByColumns.map(col):_*)
      .agg(
        min(col(ORIGINAL_VALUE)).as(SAMPLE_MIN),
        max(col(ORIGINAL_VALUE)).as(SAMPLE_MAX),
        first(col(ORIGINAL_VALUE)).as(SAMPLE_FIRST),
        last(col(ORIGINAL_VALUE)).as(SAMPLE_LAST),
        count(col(FEATURE_VALUE)).as(COUNT)
      )
  }


}
