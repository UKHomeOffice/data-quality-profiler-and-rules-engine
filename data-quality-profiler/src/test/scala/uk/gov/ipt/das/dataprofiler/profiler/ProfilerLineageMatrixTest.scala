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

class ProfilerLineageMatrixTest extends AnyFunSpec with SparkSessionTestWrapper{
  it("test generating summary statistics on a dataframe"){
    def templateJsonStr(id: String, path1: String, value1: String, event: String, date: String): String = {
      s"""{
         |  "id": "$id",
         |  "$path1": "$value1",
         |  "event" : "${event}",
         |  "date": "$date"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", path1 = "foo", value1 = "Ed",event = "update", date = "2020-03-19T07:47:52.186" ),
      templateJsonStr(id = "RECORD1", path1 = "foo", value1 = "James", event = "delete", date = "2021-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD2", path1 = "bar", value1 = "Laurence", event = "insert", date = "2022-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD3", path1 = "foo", value1 = "Dan",event = "delete", date = "2019-03-19T07:47:52.186" ),
      templateJsonStr(id = "RECORD4", path1 = "foo", value1 = "", event = "insert", date = "2021-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD5", path1 = "bar", value1 = "Andrew", event = "delete", date = "2018-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD6", path1 = "baz", value1 = "John",event = "insert", date = "2017-03-19T07:47:52.186" ),
      templateJsonStr(id = "RECORD7", path1 = "foo", value1 = "James", event = "update", date = "2016-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD8", path1 = "bar", value1 = "Peter", event = "update", date = "2015-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD9", path1 = "foo", value1 = "Ed",event = "delete", date = "2014-03-19T07:47:52.186" ),
      templateJsonStr(id = "RECORD10", path1 = "baz", value1 = "", event = "insert", date = "2012-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD11", path1 = "bar", value1 = "Dan", event = "delete", date = "2021-08-19T07:47:52.186"),
      templateJsonStr(id = "RECORD12", path1 = "baz", value1 = "", event = "insert", date = "2012-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD13", path1 = "bar", value1 = "Dan", event = "delete", date = "2021-08-19T07:47:52.186"),
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"),
        identifierPaths = IdentifierPaths(
          "event" -> IdentifierSource.direct("event")))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("foo$", "bar$")
      ).executeMulti().head._2
    profiledRecords.getProfiledData.dataFrame.get.show(false)

    // add logic to pivot dataframe and create matrix here
    profiledRecords
      .getProfiledData
      .dataFrame
      .get
      .stat
      .crosstab("featurePath", "event")
      .orderBy("featurePath_event")
      .show(false)

  }
}
