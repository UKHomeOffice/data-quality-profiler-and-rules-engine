package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{CompoundPathExistsInRecord, CompoundRule, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class CompoundPathExistsInRecordTest extends AnyFunSpec with SparkSessionTestWrapper {


  it("Executes Compound Path Exists in record test") {

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
          Seq(
            CompoundPathExistsInRecord(
              definitionName = "CompoundPathExistsInRecord",
              paths = Seq("v1", "v3")
            ),
            CompoundPathExistsInRecord(
              definitionName = "CompoundPathExistsInRecord",
              paths = Seq("v1", "v2")
            )
          ):_*
      ).executeMulti()

    val results1 = results.head._2
    val results2 = results(1)._2

    val profiledData1 = results1.getProfiledData.dataFrame
    profiledData1.get.show(numRows = 1000, truncate = false)

    val featurePoints1 = results1.featurePoints.collect()

    val records1 = results1.featurePoints.collect()

    println(featurePoints1.mkString("\n"))

    assert(featurePoints1.length === 3)

    assert(featurePoints1.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === BooleanValue(false))
    assert(featurePoints1.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === BooleanValue(false))
    assert(featurePoints1.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === BooleanValue(false))

    val profiledData2 = results2.getProfiledData.dataFrame
    profiledData2.get.show(numRows = 1000, truncate = false)

    val featurePoints2 = results2.featurePoints.collect()

    val records2 = results2.featurePoints.collect()

    println(featurePoints2.mkString("\n"))

    assert(featurePoints2.length === 3)

    assert(featurePoints2.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === BooleanValue(true))
    assert(featurePoints2.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === BooleanValue(true))
    assert(featurePoints2.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === BooleanValue(true))
  }

}
