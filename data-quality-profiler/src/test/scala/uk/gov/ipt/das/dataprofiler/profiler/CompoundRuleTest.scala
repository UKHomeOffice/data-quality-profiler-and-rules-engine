package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{CompoundRule, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class CompoundRuleTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Executes Compound Rule") {

    def templateJsonStr(id: String, v1: String, v2: String): String = {
      s"""{
         |  "id": "$id",
         |  "v1": "$v1",
         |  "v2": "$v2"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", v1 = "foo", v2 = "foo"),
      templateJsonStr(id = "RECORD1", v1 = "foo", v2 = "bar"),
      templateJsonStr(id = "RECORD2", v1 = "bar", v2 = "bar"),
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id")))
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompoundRule(
            definitionName = "CompoundRule",
            rules = Seq(("v1", "MaxLength3"), ("v2", "OnlyPermittedCharacters-allAlpha"))
          ).withFilterByPaths("v1", "v2")
      ).executeMulti().head._2

    val profiledData = results.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val featurePoints = results.featurePoints.collect()

    println(featurePoints.mkString("\n"))

    assert(featurePoints.length === 3)

    assert(featurePoints.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === BooleanValue(true))
    assert(featurePoints.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === BooleanValue(true))
    assert(featurePoints.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === BooleanValue(true))
  }

  it("Executes Compound Rule with missing path") {

    def templateJsonStr(id: String, v1: String, v2: String): String = {
      s"""{
         |  "id": "$id",
         |  "v1": "$v1",
         |  "v2": "$v2"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", v1 = "foo", v2 = "foo"),
      templateJsonStr(id = "RECORD1", v1 = "foo", v2 = "bar"),
      templateJsonStr(id = "RECORD2", v1 = "bar", v2 = "bar"),
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id")))
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompoundRule(
            definitionName = "CompoundRule",
            rules = Seq(("v1", "MaxLength3"), ("v3", "OnlyPermittedCharacters-allAlpha"))
          ).withFilterByPaths("v1", "v3")
      ).executeMulti().head._2

    val profiledData = results.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val featurePoints = results.featurePoints.collect()

    val records1 = results.featurePoints.collect()

    println(featurePoints.mkString("\n"))

    assert(featurePoints.length === 3)

    assert(featurePoints.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === BooleanValue(false))
    assert(featurePoints.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === BooleanValue(false))
    assert(featurePoints.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === BooleanValue(false))
  }

}
