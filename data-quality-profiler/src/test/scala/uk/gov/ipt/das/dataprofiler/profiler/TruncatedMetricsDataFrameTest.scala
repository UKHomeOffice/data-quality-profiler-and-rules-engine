package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.IdentifierPaths
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class TruncatedMetricsDataFrameTest extends AnyFunSpec with SparkSessionTestWrapper {
  it("test generating metrics on a dataframe then limiting output records") {
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
      templateJsonStr(id = "RECORD0", path1 = "foo", value1 = "foo", event = "1", date = "2020-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD1", path1 = "foo", value1 = "", event = "2", date = "2020-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD2", path1 = "bar", value1 = "foo", event = "2", date = "2020-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD3", path1 = "foo", value1 = "", event = "2", date = "2020-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD4", path1 = "bar", value1 = "fodo", event = "2", date = "2020-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD5", path1 = "foo", value1 = "", event = "2", date = "2020-02-21T09:47:52.186"),
      templateJsonStr(id = "RECORD6", path1 = "bar", value1 = "fdoo", event = "3", date = "2020-03-19T07:47:52.186"),
      templateJsonStr(id = "RECORD7", path1 = "bar", value1 = "barbaz", event = "3", date = "2020-03-19T07:47:52.186"),
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"),
        identifierPaths = IdentifierPaths(
          "event" -> IdentifierSource.direct("event"),
          "date" -> IdentifierSource.direct("date")
        ))
      )
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(profilers = MaskProfiler.defaultProfilers: _*)
            .withFilterByPaths("foo$", "bar$")
      ).executeMulti().head._2
    val metricsDf = profiledRecords.getMetrics()
    println("Metrics Dataframe: ")
    metricsDf.dataFrame.get.show()

    val (twoLimitDf, summaryTwo)  = metricsDf.truncate(2, spark)
    val (oneLimitDf,  summaryOne) = metricsDf.truncate(1, spark)
    println("Truncated with record limit 2:")
    twoLimitDf.get.show(false)
    println("Summary of above truncation:")
    summaryTwo.get.show(truncate=false)

    println("Truncated with record limit 1:")
    oneLimitDf.get.show(false)
    println("Summary of above truncation:")
    summaryOne.get.show(false)

    //assertions
    assert(twoLimitDf.get.count() === 8)
    assert(summaryTwo.get.count() === 1)
    assert(oneLimitDf.get.count() === 5)
    assert(summaryOne.get.count() === 3)
  }
}