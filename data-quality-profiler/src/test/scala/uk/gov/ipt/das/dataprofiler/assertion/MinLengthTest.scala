package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltInFunction
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class MinLengthTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("MinLength assertion calculated correctly") {

    def recordFromValue(id: String, value: String): String =
      s"""{
         | "id": "$id",
         | "value": "$value"
         |}
         |""".stripMargin

    val testDataset = Seq(
      recordFromValue("SOME_ID_1", "f"),
      recordFromValue("SOME_ID_2", "fo"),
      recordFromValue("SOME_ID_3", "bar"),
      recordFromValue("SOME_ID_4", "quxx"),
      recordFromValue("SOME_ID_5", "quxxx"),
    )

    val recordSets = RecordSets("test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))))

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(BuiltInFunction(name = "MinLength3"))
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
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_1' AND
         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_2' AND
         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_3' AND
         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "true")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_4' AND
         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "true")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_5' AND
         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "true")

  }

}
