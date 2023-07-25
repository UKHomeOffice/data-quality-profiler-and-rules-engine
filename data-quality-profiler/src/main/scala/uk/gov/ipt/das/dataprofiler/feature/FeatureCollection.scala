package uk.gov.ipt.das.dataprofiler.feature

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.{MetricsDataFrame, ProfiledDataFrame}
import uk.gov.ipt.das.dataprofiler.feature.combiner.Combined
import uk.gov.ipt.das.dataprofiler.writer.DataFrameWriter

case class FeatureCollection private (featurePoints: Dataset[FeaturePoint]) {

  protected def getFeaturePoints: Dataset[FeaturePoint] = featurePoints

  def join(anotherFeatureCollection: FeatureCollection): FeatureCollection =
    FeatureCollection(featurePoints.union(anotherFeatureCollection.getFeaturePoints))

  /**
   * Convert each record into a nested schema-on-read JSON object
   *
   * @param pretty Pretty print the JSON strings
   * @return A Dataset of (recordId, JSONString)
   */
  def asJSONStrings(pretty: Boolean = false): Dataset[(String, String)] =
    FeatureCollectionJSONEncoder.toJSONStrings(featureCollection = this, pretty = pretty)

  def getProfiledData: ProfiledDataFrame =
    ProfiledDataFrame(this)

  /**
   * Get the metrics as a DataFrame
   *
   * @return DataFrame with Schema from FeatureReportRow
   */
  def getMetrics(additionalGroupByColumns: String*): MetricsDataFrame =
    MetricsDataFrame(additionalGroupByColumns, generateFeatureReportRows(additionalGroupByColumns))

  private def generateFeatureReportRows(additionalGroupByColumns: Seq[String]): Dataset[FeatureReportRow] = {

    // create FeatureReportRows (do the counting and keep results in spark) and group by path
    // TODO remove use of RDD, keep it as a Dataset is possible/useful
    val combinedRDD: JavaPairRDD[ComparableFeaturePoint, Combined] = JavaPairRDD.fromJavaRDD(
      featurePoints.toJavaRDD.map { featurePoint =>
          (featurePoint.toComparable(additionalGroupByColumns), Combined.make(featurePoint.originalValue))
        }: JavaRDD[(ComparableFeaturePoint, Combined)]
      )

    val reducedRDD = combinedRDD.reduceByKey{ (l1: Combined, l2: Combined) => l1.reduce(l2) }
    val reportRows = reducedRDD.map(entry => {
      val (featurePoint, combined) = entry
      combined.toFeatureReportRow(featurePoint)
    })

    featurePoints.sparkSession.createDataset(reportRows)
  }

  def getReportRows: Seq[FeatureReportRow] =
    generateFeatureReportRows(additionalGroupByColumns = Seq()).collect()

  def writeAll(writer: DataFrameWriter): Unit = {
    getProfiledData.write(writer, partitionOn = None)
    getMetrics().write(writer, partitionOn = None)
  }


}
@SuppressWarnings(Array("org.wartremover.warts.IterableOps","org.wartremover.warts.Var"))
object FeatureCollection {
  def apply(featurePoints: Dataset[FeaturePoint]): FeatureCollection =
    new FeatureCollection(featurePoints = featurePoints)

  def apply(featureCollections: Seq[FeatureCollection]): FeatureCollection = {
    var gatheredFc = featureCollections.head
    featureCollections.drop(1).foreach { newFc => gatheredFc = gatheredFc.join(newFc) }
    gatheredFc
  }
}
