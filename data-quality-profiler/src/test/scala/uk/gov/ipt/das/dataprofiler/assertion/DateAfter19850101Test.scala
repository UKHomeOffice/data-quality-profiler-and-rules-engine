package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltInFunction
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class DateAfter19850101Test extends AnyFunSpec with SparkSessionTestWrapper {
  it("DateAfter19850101Test assertion calculated correctly") {

    def recordFromValue(id: String, value: String): String = {
      val wrappedStr =
        if (value == null) {
          "null"
        } else {
          "\"" + value + "\""
        }

      s"""{
         | "id": "$id",
         | "value": $wrappedStr
         |}
         |""".stripMargin
    }

    val testDataset = Seq(
      recordFromValue("RECORD1", "13-07-1984"),
      recordFromValue("RECORD2", "XXXXXX"),
      recordFromValue("RECORD3", null),
      recordFromValue("RECORD4", "13-07-2021") ,
      recordFromValue("RECORD5", "13-07-1800")
    )

    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(BuiltInFunction(name = "DateAfter19850101"))
            .withFilterByPaths("^value$")
      ).executeMulti().head._2


    // generate+show metrics for reference
    results.getMetrics().dataFrame.get.show(numRows = 1000, truncate = false)

    // assert that assertions are as expected
    println("Starting assertions")
    val profiledData = results.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val assertionsView = datasetView(profiledData.get)

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD1' AND
         | featureName = 'DQ_DateAfter19850101'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD2' AND
         | featureName = 'DQ_DateAfter19850101'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD3' AND
         | featureName = 'DQ_DateAfter19850101'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD4' AND
         | featureName = 'DQ_DateAfter19850101'""".stripMargin).first().getString(0) === "true")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD5' AND
         | featureName = 'DQ_DateAfter19850101'""".stripMargin).first().getString(0) === "false")

  }
}
