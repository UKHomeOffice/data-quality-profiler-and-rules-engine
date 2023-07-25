package uk.gov.ipt.das.dataprofiler.dataframe.profiler.output

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollectionEncoder._
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollection

case class ProfiledDataFrame private (dataFrameIn: DataFrame,
                                      viewName: String = ReferenceDataset.randomViewName)
  extends VersionedDataFrame {

  override lazy val dataFrame: Option[DataFrame] = Option(dataFrameIn)

  dataFrameIn.createOrReplaceTempView(viewName)

  override val schemaVersion: String = "schemaV4"
  override val name: String = "profiler-masks-with-original-data"

  def withViewName(newViewName: String): ProfiledDataFrame = copy(viewName = newViewName)

  private def filterBy(column: String, value: String): ProfiledDataFrame =
    ProfiledDataFrame(dataFrameIn.filter(col(column) === value))

  def filterByPath(featurePath: String): ProfiledDataFrame = filterBy(FEATURE_PATH, featurePath)

  def filterByFeatureName(featureName: String): ProfiledDataFrame = filterBy(FEATURE_NAME, featureName)

}

/**
 * DataFrame of profiled records, either from FeatureCollection.getDataFrame (i.e., when
 * DataProfiler().profile() has been run, or from storage by running spark.read.parquet()
 *
 * Therefore this class must deal with additional columns as additional identifiers.
 *
 */
object ProfiledDataFrame {
  def apply(featureCollection: FeatureCollection): ProfiledDataFrame =
    new ProfiledDataFrame(toDataFrame(featureCollection))

  def apply(dataFrame: DataFrame): ProfiledDataFrame =
    new ProfiledDataFrame(dataFrame)
}
