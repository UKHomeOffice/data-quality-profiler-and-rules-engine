package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{Comparator, CompareDateToEventDate, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.Comparator.BEFORE
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class CompareDateToEventDateTest extends AnyFunSpec with SparkSessionTestWrapper {
  it("test comparing date to event date") {
    def recordFromValue(id: String, value: String): String = {
      val wrappedStr =
        if (value == null) {
          "null"
        } else {
          "\"" + value + "\""
        }

      s"""{
         | "id": "$id",
         | "value": $wrappedStr,
         | "event_date": "2020-01-23T09:37:23.641"
         |}
         |""".stripMargin
    }

    val testDataset = Seq(
      recordFromValue("RECORD1", "2019-01-23T09:37:23.641"),
    )


    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompareDateToEventDate(
            definitionName = "DateNotAfterEventDate",
            datePath = "value",
            comparator = BEFORE
          )
      ).executeMulti().head._2


    // generate+show metrics for reference
    val df = results.getMetrics().dataFrame.get
    df.show(numRows = 1000, truncate = false)

    //assertions
    assert(df.select("featureValue").first().getString(0) === "true")

  }

  it("test comparing dodgy input date to event date") {
    def recordFromValue(id: String, value: String): String = {
      val wrappedStr =
        if (value == null) {
          "null"
        } else {
          "\"" + value + "\""
        }

      s"""{
         | "id": "$id",
         | "value": $wrappedStr,
         | "event_date": "2020-01-23T09:37:23.641"
         |}
         |""".stripMargin
    }

    val testDataset = Seq(
      recordFromValue("RECORD1", "2019"),
    )


    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompareDateToEventDate(
            definitionName = "DateNotAfterEventDate",
            datePath = "value",
            comparator = Comparator.BEFORE
          )
      ).executeMulti().head._2


    // generate+show metrics for reference
    val df = results.getMetrics().dataFrame.get
    df.show(numRows = 1000, truncate = false)

    //assertions
    assert(df.select("featureValue").first().getString(0) === "ERROR")

  }

}
