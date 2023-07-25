package uk.gov.ipt.das.dataprofiler.profiler.input.record

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.value.StringValue

@SuppressWarnings(Array("org.wartremover.warts.IterableOps","org.wartremover.warts.NonUnitStatements"))
object FlattenedProfilableRecordEncoder {

  val RECORD_ID = "recordId"
  val FLAT_PATH = "flatPath"
  val FULLY_QUALIFIED_PATH = "fullyQualifiedPath"
  val RECORD_VALUE = "recordValue"

  val baseColumnOrder = Seq(
    RECORD_ID,
    FLAT_PATH,
    FULLY_QUALIFIED_PATH,
    RECORD_VALUE,
  )

  def fromDataFrame(dataFrame: DataFrame): FlattenedRecords = {
    import dataFrame.sparkSession.implicits._

    val fieldNames = dataFrame.head().schema.fieldNames
    val additionalIdentifierNames = fieldNames.slice(baseColumnOrder.length, fieldNames.length)

    def toAdditionalIdentifiers(row: Row): AdditionalIdentifiers = {
      if (additionalIdentifierNames.isEmpty) {
        AdditionalIdentifiers()
      } else {
        AdditionalIdentifiers(
          values = additionalIdentifierNames.zipWithIndex.map{ case (name, index) =>
            AdditionalIdentifier(
              name = name,
              value = row.getString(index + baseColumnOrder.length)
            )
          }.toList
        )
      }
    }

    FlattenedRecords(dataFrame.groupByKey(row => row.getString(0))
      .mapGroups{ case( recordId, rowIterator) =>
        val rows = rowIterator.toSeq
        FlattenedProfilableRecord(
          id = recordId,
          flatValues = rows.map{ row =>
            FlatValue(
              flatPath = row.getString(1),
              fullyQualifiedPath = row.getString(2),
              recordValue = StringValue(row.getString(3))
            )
          },
          additionalIdentifiers = toAdditionalIdentifiers(rows.take(1).head)
        )
      })
  }

  def toDataFrame(records: Dataset[FlattenedProfilableRecord]): DataFrame = {

    if (records.isEmpty) {
       records.sparkSession.createDataFrame(List.empty)
    }

    //TODO creation of the schema should be done once per run, based on identityPaths being
    // sent somewhere early on, rather than generated dynamically only when the dataframe is created

    val columnOrder = baseColumnOrder ++ records.head.additionalIdentifiers.values.map{ _.name }

    val schema = StructType(columnOrder.map { colName => StructField(colName, StringType) })

    val dataset = records.flatMap { fpr =>
      fpr.flatValues.map { value =>
        val params = Seq(
          fpr.id,
          value.flatPath,
          value.fullyQualifiedPath,
          value.recordValue.valueAsString
        ) ++ fpr.additionalIdentifiers.values.map { _.value }

        Row(params: _*)
      }
    }(RowEncoder(schema))

    dataset.toDF()
  }


}
