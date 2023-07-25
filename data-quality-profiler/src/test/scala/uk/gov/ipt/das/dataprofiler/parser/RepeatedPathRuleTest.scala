package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltInFunction
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class RepeatedPathRuleTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("repeated array-indexed paths are handled correctly") {
    def recordFromValue(id: String, value1: String, value2: String, value3: String): String =
      s"""{
         | "id": "$id",
         | "value": ["$value1", "$value2", "$value3"]
         |}
         |""".stripMargin

    val testDataset = Seq(
      recordFromValue("SOME_ID_1", "f", "f2", "f3"),
      recordFromValue("SOME_ID_2", "fo", "f2", "foo"),
      recordFromValue("SOME_ID_3", "bar", "f", "f"),
      recordFromValue("SOME_ID_4", "qux", "0", "1"),
      recordFromValue("SOME_ID_5", "quxxx", "_", "aa"),
    )

    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(BuiltInFunction(name = "MinLength3"))
            .withFilterByPaths("^value\\[\\]$")
      ).executeMulti().head._2

    // generate+show metrics for reference
    results.getMetrics().dataFrame.get.show(numRows = 1000, truncate = false)

    // assert that assertions are as expected
    println("Starting assertions")
    val profiledData = results.getProfiledData.dataFrame.get
    profiledData.show(numRows = 1000, truncate = false)

    assert(profiledData.count() === 15) // there should be 1 minLength3 rule per value

//    val assertionsView = datasetView(profiledData)
//
//    assert(spark.sql(
//      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_1' AND
//         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "false")
//
//    assert(spark.sql(
//      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_2' AND
//         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "false")
//
//    assert(spark.sql(
//      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_3' AND
//         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "true")
//
//    assert(spark.sql(
//      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_4' AND
//         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "true")
//
//    assert(spark.sql(
//      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'SOME_ID_5' AND
//         | featureName = 'DQ_MinLength3'""".stripMargin).first().getString(0) === "true")
  }

}
