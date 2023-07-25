package uk.gov.ipt.das.dataprofiler.profiler

import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.parser.WritePerformanceTestRun.tmpDir
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.dataframe.summary.SummaryDataFrame
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.IdentifierPaths
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper
import uk.gov.ipt.das.dataprofiler.writer.LocalFileWriter

class ProfilerSummaryStatisticsTest extends AnyFunSpec with SparkSessionTestWrapper{
  it("test generating summary statistics on a dataframe"){
    def templateJsonStr(id: String, path1: String, value1: String, event: String, date: String): String = {
      s"""{
         |  "id": "$id",
         |  "$path1": "$value1",
         |  "event" : "$event",
         |  "date": "$date"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", path1 = "foo", value1 = "foo",event = "1", date = "2020-03-19T07:47:52.186" ),
      templateJsonStr(id = "RECORD1", path1 = "foo", value1 = "", event = "2", date = "2020-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD2", path1 = "bar", value1 = "foo", event = "2", date = "2020-03-19T07:47:52.186"),
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"),
        identifierPaths = IdentifierPaths(
          "event" -> IdentifierSource.direct("event"),
          "date" -> IdentifierSource.direct("date")))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("foo$", "bar$")
      ).executeMulti().head._2
    profiledRecords.getProfiledData.dataFrame.get.show(false)

    val eventSummaryDf = SummaryDataFrame(fieldToSummarise = "event",
      dataFrameIn =  profiledRecords.getProfiledData, transformDateField = false).dataFrame.get

    val dateSummaryDf = SummaryDataFrame(fieldToSummarise = "date",
      dataFrameIn =  profiledRecords.getProfiledData, transformDateField = true).dataFrame.get

    val dataFrameInSummaryDf = SummaryDataFrame(fieldToSummarise = "date",
      dataFrameIn =  profiledRecords.getProfiledData.dataFrame.get, transformDateField = true).dataFrame.get


    eventSummaryDf.show(false)

    dateSummaryDf.show(false)

    //assertions
    assert(eventSummaryDf
      .filter(col("event") === 1)
      .select(col("count")).first().getLong(0) === 1)

    assert(eventSummaryDf
      .filter(col("event") === 2)
      .select(col("count")).first().getLong(0) === 2)

    assert(dateSummaryDf
      .filter(col("date-YearMonth") === "2020-02")
      .select(col("count")).first().getLong(0) === 1)

    assert(dateSummaryDf
      .filter(col("date-YearMonth") === "2020-03" )
      .select(col("count")).first().getLong(0) === 2)

    //test the different apply methods
    assert(dateSummaryDf.columns.length === dataFrameInSummaryDf.columns.length)
    assert(dateSummaryDf.count() === dataFrameInSummaryDf.count())

  }
  it("test writing summary outputs") {
    def templateJsonStr(id: String, path1: String, value1: String, event: String, date: String): String = {
      s"""{
         |  "id": "$id",
         |  "$path1": "$value1",
         |  "event" : "$event",
         |  "date": "$date"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", path1 = "foo", value1 = "foo",event = "1", date = "2020-03-19T07:47:52.186" ),
      templateJsonStr(id = "RECORD1", path1 = "foo", value1 = "", event = "2", date = "2020-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD2", path1 = "bar", value1 = "foo", event = "2", date = "2020-03-19T07:47:52.186"),
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"),
        identifierPaths = IdentifierPaths(
          "event" -> IdentifierSource.direct("event"),
          "date" -> IdentifierSource.direct("date"))))
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("foo$", "bar$")
      ).executeMulti().head._2
    profiledRecords.getProfiledData.dataFrame.get.show(false)

    val eventSummary = SummaryDataFrame(fieldToSummarise = "event",
      dataFrameIn =  profiledRecords.getProfiledData
      ,transformDateField = false)
    val tempOutput = tmpDir(prefix = "summary_out_location")
    println(s"written to: $tempOutput")
    eventSummary.write(LocalFileWriter(tempOutput), partitionOn = None)
    val df = spark.read.parquet(s"$tempOutput/*")
    df.show(false)

    //assertions
    assert(df
      .filter(col("event") === 1)
      .select(col("count")).first().getLong(0) === 1)

    assert(df
      .filter(col("event") === 2)
      .select(col("count")).first().getLong(0) === 2)
  }
}
