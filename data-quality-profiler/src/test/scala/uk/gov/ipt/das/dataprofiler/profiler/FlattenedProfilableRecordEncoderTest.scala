package uk.gov.ipt.das.dataprofiler.profiler

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.IdentifierPaths
import uk.gov.ipt.das.dataprofiler.profiler.input.record.{FlattenedProfilableRecordEncoder, FlattenedRecords}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class FlattenedProfilableRecordEncoderTest extends AnyFunSpec with SparkSessionTestWrapper with DatasetComparer {

  it("writes and reads flattened profilable records to/from dataframes") {
    def templateJsonStr(id: String, value1: String, value2: String): String = {
      s"""{
         |  "id": "$id",
         |  "path1": "$value1",
         |  "path2": "$value2"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      // path1 all UNIQUE, path2 RECORD0 and RECORD2 NOT unique
      templateJsonStr(id = "RECORD0", value1 = "foo", value2 = "foo"),
      templateJsonStr(id = "RECORD1", value1 = "bar", value2 = "bar"),
      templateJsonStr(id = "RECORD2", value1 = "baz", value2 = "foo"),
      templateJsonStr(id = "RECORD3", value1 = "foo", value2 = "foo"),
      templateJsonStr(id = "RECORD4", value1 = "qux", value2 = "bar"),
      templateJsonStr(id = "RECORD5", value1 = "quxx", value2 = "foo"),
    )

    val records = FlattenedRecords(fromJsonStrings(
      spark,
      jsonRecords,
      idPath = Option("id"),
      identifierPaths = IdentifierPaths(
        "path1" -> IdentifierSource.direct("path1"),
        "path2" -> IdentifierSource.direct("path2")
      )
    ))

    println("Dataset of source records")
    records.records.collect().foreach { println }

    val asDf = FlattenedProfilableRecordEncoder.toDataFrame(records.records)
    println("Dataframe of source records")
    asDf.show(truncate = false, numRows = 1000)

    val recordFromDf = FlattenedProfilableRecordEncoder.fromDataFrame(asDf)
    println("Dataset of decoded records")
    recordFromDf.records.collect().foreach { println }

    val asDf2 = FlattenedProfilableRecordEncoder.toDataFrame(recordFromDf.records)
    println("Dataframe of source records again")
    asDf2.show(truncate = false, numRows = 1000)
  }

}
