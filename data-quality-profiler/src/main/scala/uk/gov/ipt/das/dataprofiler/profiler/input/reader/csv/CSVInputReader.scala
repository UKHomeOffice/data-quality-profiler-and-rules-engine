package uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
import uk.gov.ipt.das.dataprofiler.profiler.input.records.ProfilableRecords

object CSVInputReader {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def reader(spark: SparkSession) =
    spark.read
      .option("header", "true")
      .option("emptyValue", "")
      .option("nullValue", null)
      .option("inferSchema", "true")

  private def toSparkRecords(csvRows: DataFrame, idColumn: String): ProfilableRecords = {
    val idColumnNumber = csvRows.schema.fieldIndex(idColumn) // do this once here rather than in the loop
    val fieldNames = csvRows.schema.fieldNames // get this out of the loop, too

    ProfilableRecords(csvRows.map { row =>
        ProfilableRecord.fromIterableAny(
          id = Option(row.get(idColumnNumber).toString),
          iterableAny = row.getValuesMap(fieldNames),
          additionalIdentifiers = AdditionalIdentifiers()) // TODO support additionalIdentifiers if necessary
      }
    )

  }

  def readCSVString(spark: SparkSession, csvStrings: Seq[String], idColumn: String): ProfilableRecords = {
    import spark.implicits._
    toSparkRecords(reader(spark).csv(spark.createDataset(csvStrings)), idColumn = idColumn)
  }

  def readCSV(spark: SparkSession, path: String, idColumn: String): ProfilableRecords =
    toSparkRecords(reader(spark).csv(path), idColumn = idColumn)
}
