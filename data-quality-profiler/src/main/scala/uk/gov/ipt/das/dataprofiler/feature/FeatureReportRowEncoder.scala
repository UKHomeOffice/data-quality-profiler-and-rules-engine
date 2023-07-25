package uk.gov.ipt.das.dataprofiler.feature

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
@SuppressWarnings(Array("org.wartremover.warts.Any","org.wartremover.warts.NonUnitStatements"))
object FeatureReportRowEncoder {

  val FEATURE_PATH: String = FeatureCollectionEncoder.FEATURE_PATH
  val FEATURE_NAME: String = FeatureCollectionEncoder.FEATURE_NAME
  val FEATURE_VALUE: String = FeatureCollectionEncoder.FEATURE_VALUE
  val SAMPLE_MIN: String = "sampleMin"
  val SAMPLE_MAX: String = "sampleMax"
  val SAMPLE_FIRST: String = "sampleFirst"
  val SAMPLE_LAST: String = "sampleLast"
  val COUNT: String = "count"

  val baseColumnOrder: Seq[String] = Seq(
    FEATURE_PATH,
    FEATURE_NAME,
    FEATURE_VALUE,
    SAMPLE_MIN,
    SAMPLE_MAX,
    SAMPLE_FIRST,
    SAMPLE_LAST,
    COUNT
  )

  def toDataFrame(additionalGroupByColumns: Seq[String], featureReportRows: Dataset[FeatureReportRow]): Option[DataFrame] = {

    if (featureReportRows.isEmpty) {
      None
    } else {

      //TODO creation of the schema should be done once per run, based on identityPaths being
      // sent somewhere early on, rather than generated dynamically only when the dataframe is created

      val columnOrder = baseColumnOrder ++ additionalGroupByColumns

      val schema = StructType(columnOrder.map { colName =>
        if (colName == "count") {
          StructField(colName, LongType)
        } else {
          StructField(colName, StringType)
        }
      })

      val dataset = featureReportRows.map { frr =>
        val params = Seq(
          frr.featurePoint.flatPath,
          frr.featurePoint.featureDefinition,
          frr.featurePoint.featureValue,
          frr.sampleMin,
          frr.sampleMax,
          frr.sampleFirst,
          frr.sampleLast,
          frr.count
        ) ++ frr.featurePoint.additionalGroupByValues

        Row(params: _*)
      }(RowEncoder(schema))

      Option(dataset.toDF())
    }
  }


}
