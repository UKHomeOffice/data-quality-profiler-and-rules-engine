package uk.gov.ipt.das.dataprofiler.dataframe.analysis

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollectionEncoder._
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.ProfiledDataFrame
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
class RefDataAnalysisDataFrame(dataFrameIn: ProfiledDataFrame
                               , featureNames: Seq[String]) extends VersionedDataFrame {
//TODO need a default for this data frame
  private lazy val aggregatedDataFrame = dataFrameIn
    .dataFrame
    .get
    .filter(col(FEATURE_NAME).isin(featureNames:_*))
    .groupBy(ORIGINAL_VALUE, FEATURE_NAME, FEATURE_VALUE)
    .count()

  override lazy val dataFrame: Option[DataFrame] = Option(aggregatedDataFrame)

  override val schemaVersion: String = "schemaV1"

  override val name: String = "profiler-ref-data-aggregated-analysis"
}

object RefDataAnalysisDataFrame {
  def apply(dataFrameIn: ProfiledDataFrame,
            featureNames: Seq[String] = Seq(
              "DQ_ValuePresentInReferenceDataset",
              "DQ_ValueNotPresentInReferenceDataset")): RefDataAnalysisDataFrame =
    new RefDataAnalysisDataFrame(dataFrameIn = dataFrameIn, featureNames = featureNames)
}

