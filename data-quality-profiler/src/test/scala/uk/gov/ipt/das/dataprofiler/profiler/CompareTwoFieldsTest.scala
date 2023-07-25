package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{CompareTwoFields, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class CompareTwoFieldsTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Executes a ProfilerConfiguration") {

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
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"))),
    )

    val profiledRecords1 =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompareTwoFields(
            definitionName = "V1_EQUALS_V2",
            pathOne = "v1",
            pathTwo = "v2",
            comparisonFunction = (val1, val2) => (val1 == val2).toString
          )
      ).executeMulti().head._2

    val records1 = profiledRecords1.featurePoints.collect()

    println(records1.mkString("\n"))

    assert(records1.length === 3)

    assert(records1.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === StringValue("true"))
    assert(records1.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === StringValue("false"))
    assert(records1.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === StringValue("true"))
  }

}
