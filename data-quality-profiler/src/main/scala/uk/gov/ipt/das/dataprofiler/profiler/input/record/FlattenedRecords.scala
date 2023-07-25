package uk.gov.ipt.das.dataprofiler.profiler.input.record

import com.dslplatform.json._
import com.dslplatform.json.runtime.Settings
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset.randomViewName
import FlattenedRecords.dslJson
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor.{KeyPreProcessor, PassthroughKeyProcessor}
import uk.gov.ipt.das.dataprofiler.profiler.input.records.ProfilableRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.ArrayQueryPath

import java.io.ByteArrayOutputStream
import java.util.regex.Pattern
case class FlattenedRecords private (records: Dataset[FlattenedProfilableRecord])  {

  def filterByPaths(pathFilters: Option[Seq[Pattern]]): FlattenedRecords =
    FlattenedRecords(records.map { record => record.filterByPaths(pathFilters) })

  def withArrayQueryPaths(arrayQueryPath: Seq[ArrayQueryPath]): FlattenedRecords = {
    FlattenedRecords(records.map {record => record.withArrayQueryPaths(arrayQueryPath)})
  }

  def addValueDerivedArrayIdentifier(arrayPath: String,
                                     valuePath: String,
                                     lookupPath: String,
                                     lookupValue: String,
                                     identifierName: String ): FlattenedRecords = {
    FlattenedRecords(records.map {record => record
      .withValueDerivedArrayIdentifier(arrayPath, valuePath,lookupPath,lookupValue, identifierName)})
  }

  def idColumn: String = "id"
  def flatPathColumn: String = "flatPath"
  def valueColumn: String = "value"
  def additionalIdentifiersColumn: String = "additionalIdentifiers"

  //TODO i dont really understand this code is this always empty at line 49??
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var tempViewName: Option[String] = None

  def toFlattenedDataFrame: DataFrame =
    FlattenedProfilableRecordEncoder.toDataFrame(records)

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def viewName: String = {
    if (tempViewName.isEmpty) {
      // Turn into a dataframe lazily - don't bother if it's not used.
      val columnOrder = Seq(idColumn, flatPathColumn, valueColumn, additionalIdentifiersColumn)
      val schema = StructType( columnOrder.map { StructField(_, StringType) } )

      val dataFrame =
        records.flatMap { record =>
          record.flatValues.map { value =>
            // serialise Additional Identifiers as Json
            val outputStream = new ByteArrayOutputStream()
            dslJson.encode[AdditionalIdentifiers](record.additionalIdentifiers, outputStream)
            val additionalIdentifiersStr = outputStream.toString("UTF-8")

            Row(record.id, value.flatPath, value.recordValue.valueAsString, additionalIdentifiersStr)
          }
        }(RowEncoder(schema))
          .toDF()

      tempViewName = Option(randomViewName)
      dataFrame.createOrReplaceTempView(tempViewName.get)
    }
    tempViewName.get
  }

}
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object FlattenedRecords {

  implicit val dslJson: DslJson[Any] = new DslJson[Any](Settings
    .withRuntime()
    .includeServiceLoader()
    .`with`(new ConfigureScala))

  def apply(profilableRecords: ProfilableRecords,
            keyPreProcessor: KeyPreProcessor = PassthroughKeyProcessor()): FlattenedRecords =
    FlattenedRecords(
      profilableRecords.records.map { profilableRecord =>
        FlattenedProfilableRecord(profilableRecord, RecordFlattener(keyPreProcessor))
      }
    )

}
