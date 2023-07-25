package uk.gov.ipt.das.dataprofiler.feature

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * Encodes a FeatureCollection to a DataFrame.
 *
 * Dynamic number of columns depending on the number of AdditionalIdentifiers included
 * in the source data.
 */
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements","org.wartremover.warts.Return"))
object FeatureCollectionEncoder {

  val RECORD_ID: String = "recordId"
  val FEATURE_PATH: String = "featurePath"
  val ORIGINAL_VALUE: String = "originalValue"
  val FEATURE_NAME: String = "featureName"
  val FEATURE_VALUE: String = "featureValue"
  val RECORD_SET: String = "recordSet"

  val baseColumnOrder: Seq[String] = Seq(
    RECORD_ID,
    FEATURE_PATH,
    ORIGINAL_VALUE,
    FEATURE_NAME,
    FEATURE_VALUE,
    RECORD_SET
  )

  def isReservedColumn(columnName: String): Boolean =
    baseColumnOrder.contains(columnName)

  def toDataFrame(featureCollection: FeatureCollection): DataFrame = {
    //TODO removing return causes the test to fail
    if (featureCollection.featurePoints.isEmpty) {
      featureCollection.featurePoints.sparkSession.createDataFrame(
        List[Row]().asJava,
        StructType(baseColumnOrder.map {
          StructField(_, StringType)
        }))
    } else {

      // TODO creation of the schema should be done once per run, based on identityPaths being
      // sent somewhere early on, rather than generated dynamically only when the dataframe is created

      val columnOrder =
        baseColumnOrder ++
          featureCollection.featurePoints.head.additionalIdentifiers.values.map {
            entry => entry.name
          }


      val schema = StructType(columnOrder.map {
        StructField(_, StringType)
      })

      val dataset = featureCollection.featurePoints.map { fp =>
        val params = Seq(
          fp.recordId,
          fp.path,
          fp.originalValue,
          fp.feature.feature.getFieldName,
          fp.feature.getValueAsString,
          fp.recordSet) ++ fp.additionalIdentifiers.values.map { entry => entry.value }

        Row(params: _*)
      }(RowEncoder(schema))

      dataset.toDF()
    }
  }
}