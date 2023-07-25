package uk.gov.ipt.das.dataprofiler.profiler.input.reader.json

import com.dslplatform.json.DslJson
import org.apache.spark.sql.{Dataset, SparkSession}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers, IdentifierSource}
import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
import uk.gov.ipt.das.dataprofiler.profiler.input.records.ProfilableRecords
import uk.gov.ipt.das.dataprofiler.value.{ArrayValue, NotFoundValue, NullValue, RecordValue, StringValue}

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
//TODO I dont think we can change this
object JsonInputReader {

  private val json = new DslJson[Object]()

  def parseString(jsonString: String,
                  idPath: Option[String] = None,
                  identifierPaths: ListMap[String, IdentifierSource] = IdentifierPaths.empty): ProfilableRecord = {
    val parsedRecord = ProfilableRecord.fromIterableAny(
      iterableAny = json.deserialize(classOf[java.util.Map[String, Any]],
        new ByteArrayInputStream(jsonString.getBytes("UTF-8"))).asScala,
      additionalIdentifiers = null)

    val withId = idPath.fold(parsedRecord) {path: String =>
      parsedRecord
        .lookup(path)
        .fold(parsedRecord) {recordvalue: RecordValue => parsedRecord.withId(recordvalue.valueAsString)}
    }

    withId.withAdditionalIdentifiers(
      AdditionalIdentifiers(
        identifierPaths.toList.map { case(identifierName: String, identifierSource: IdentifierSource) =>
          AdditionalIdentifier(identifierName, withId.lookup(identifierSource.sourcePath).fold(NotFoundValue.toString) {
            case ArrayValue(values) => values.map{ value => identifierSource.processor.process(value.valueAsString)}.mkString(",")
            case StringValue(str) => identifierSource.processor.process(str)
            case NullValue() => NotFoundValue.toString
          })
        }
      )
    )
  }

  private def parseFromSpark(input: Dataset[String],
                             idPath: Option[String],
                             identifierPaths: ListMap[String, IdentifierSource]): ProfilableRecords =
    ProfilableRecords(input.map { jsonString => parseString(jsonString, idPath, identifierPaths) })

  def fromLocation(spark: SparkSession,
                   location: String,
                   idPath: Option[String],
                   oneRecordPerFile: Boolean = true,
                   identifierPaths: ListMap[String, IdentifierSource] = IdentifierPaths.empty): ProfilableRecords = {
    val readOption = if (oneRecordPerFile) "true" else "false"
    parseFromSpark(
      input = spark.read.option("wholetext", readOption).textFile(location),
      idPath = idPath,
      identifierPaths = identifierPaths
    )
  }

  def fromJsonStrings(spark: SparkSession,
                      jsonStrings: Seq[String],
                      idPath: Option[String],
                      identifierPaths: ListMap[String, IdentifierSource] = IdentifierPaths.empty): ProfilableRecords = {
    import spark.implicits._
    parseFromSpark(spark.createDataset(jsonStrings), idPath = idPath, identifierPaths = identifierPaths)
  }
}