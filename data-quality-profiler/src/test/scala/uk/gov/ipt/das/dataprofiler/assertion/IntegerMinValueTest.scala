package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltInFunction
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class IntegerMinValueTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("handles different input formats of ints when running profiler rule") {
    val testDataset = Seq(
        s"""{
           | "id": "RECORD0",
           | "value": null
           |}
           |""".stripMargin,
      s"""{
         | "id": "RECORD1",
         | "value": "10"
         |}
         |""".stripMargin,
      s"""{
         | "id": "RECORD2",
         | "value": 10
         |}
         |""".stripMargin,
      s"""{
         | "id": "RECORD3",
         | "value": 10.1
         |}
         |""".stripMargin,
        )

    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(BuiltInFunction(name = "IntegerAtLeast10"))
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
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD0' AND
         | featureName = 'DQ_IntegerAtLeast10'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD1' AND
         | featureName = 'DQ_IntegerAtLeast10'""".stripMargin).first().getString(0) === "true")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD2' AND
         | featureName = 'DQ_IntegerAtLeast10'""".stripMargin).first().getString(0) === "true")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD3' AND
         | featureName = 'DQ_IntegerAtLeast10'""".stripMargin).first().getString(0) === "true")

  }

}
