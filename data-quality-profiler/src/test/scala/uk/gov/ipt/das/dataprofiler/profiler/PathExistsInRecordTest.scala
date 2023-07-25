package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{PathExistsInRecord, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class PathExistsInRecordTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("write out an assertion when a path filter doesn't find a value") {
    def templateJsonStr(id: String, path1: String, value1: String): String = {
      s"""{
         |  "id": "$id",
         |  "$path1": "$value1"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", path1 = "foo", value1 = "foo"), // pathPresent foo == true
      templateJsonStr(id = "RECORD1", path1 = "foo", value1 = ""), // pathPresent foo == true
      templateJsonStr(id = "RECORD2", path1 = "bar", value1 = "foo"), // pathPresent foo == false
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"))),
    )

    val profiledRecords1 =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          PathExistsInRecord(
            definitionName = "PATH_FOO_IS_PRESENT",
            flatPaths = "foo"
          )
      ).executeMulti().head._2

    val records1 = profiledRecords1.featurePoints.collect()

    println(records1.mkString("\n"))

    assert(records1.length === 3)

    assert(records1.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === BooleanValue(false))
  }

  it("mix and match multiple path exists in a single rule") {
    def templateJsonStr(id: String, path1: String, value1: String): String = {
      s"""{
         |  "id": "$id",
         |  "$path1": "$value1"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", path1 = "foo", value1 = "foo"), // pathPresent foo == true
      templateJsonStr(id = "RECORD1", path1 = "bar", value1 = ""), // pathPresent bar == true
      templateJsonStr(id = "RECORD2", path1 = "qux", value1 = "foo"), // pathPresent qux == false
      templateJsonStr(id = "RECORD3", path1 = "quxx", value1 = "foo"), // pathPresent quxx == false
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"))),
    )

    val profiledRecords1 =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          PathExistsInRecord(
            definitionName = "MULTIPLE_PATHS_ARE_PRESENT",
            flatPaths = "foo", "bar"
          )
      ).executeMulti().head._2

    val records1 = profiledRecords1.featurePoints.collect()

    println(records1.mkString("\n"))

    assert(records1.length === 8)

    assert(records1.filter{ fp => fp.recordId == "RECORD0" && fp.path == "foo" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD0" && fp.path == "bar" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD1" && fp.path == "foo" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD1" && fp.path == "bar" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD2" && fp.path == "foo" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD2" && fp.path == "bar" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD3" && fp.path == "foo" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD3" && fp.path == "bar" }.head.feature.value === BooleanValue(false))
  }

}
