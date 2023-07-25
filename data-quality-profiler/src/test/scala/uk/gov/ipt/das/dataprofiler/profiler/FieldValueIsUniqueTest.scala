package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldValueIsUnique, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class FieldValueIsUniqueTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Records if a field value is unique or not") {
    def templateJsonStr(id: String, value1: String, value2: String): String = {
      s"""{
         |  "id": "$id",
         |  "path1": "$value1",
         |  "path2": "$value2"
         |}
         |""".stripMargin
    }

    val jsonRecords = Seq(
      // path1 all UNIQUE, path2 RECORD0 and RECORD2 NOT unique
      templateJsonStr(id = "RECORD0", value1 = "foo", value2 = "foo"),
      templateJsonStr(id = "RECORD1", value1 = "bar", value2 = "bar"),
      templateJsonStr(id = "RECORD2", value1 = "baz", value2 = "foo"),
      templateJsonStr(id = "RECORD3", value1 = "foo", value2 = "foo"),
      templateJsonStr(id = "RECORD4", value1 = "qux", value2 = "bar"),
      templateJsonStr(id = "RECORD5", value1 = "quxx", value2 = "foo"),
    )

    val recordSets = RecordSets("testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"))))

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldValueIsUnique(
            definitionName = "PATH1_VALUE_IS_UNIQUE",
            path = "path1"
          ),
          FieldValueIsUnique(
            definitionName = "PATH2_VALUE_IS_UNIQUE",
            path = "path2"
          )
      ).executeMulti()

    val records1 = profiledRecords(0)._2.featurePoints.collect()
    val records2 = profiledRecords(1)._2.featurePoints.collect()

    println(records1.mkString("\n"))
    println(records2.mkString("\n"))

    assert(records1.length === 6)
    assert(records2.length === 6)

    assert(records1.filter{ fp => fp.recordId == "RECORD0" && fp.feature.feature.name == "PATH1_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD1" && fp.feature.feature.name == "PATH1_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD2" && fp.feature.feature.name == "PATH1_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD3" && fp.feature.feature.name == "PATH1_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD4" && fp.feature.feature.name == "PATH1_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD5" && fp.feature.feature.name == "PATH1_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(true))

    assert(records2.filter{ fp => fp.recordId == "RECORD5" && fp.feature.feature.name == "PATH2_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records2.filter{ fp => fp.recordId == "RECORD0" && fp.feature.feature.name == "PATH2_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records2.filter{ fp => fp.recordId == "RECORD1" && fp.feature.feature.name == "PATH2_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records2.filter{ fp => fp.recordId == "RECORD2" && fp.feature.feature.name == "PATH2_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records2.filter{ fp => fp.recordId == "RECORD3" && fp.feature.feature.name == "PATH2_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
    assert(records2.filter{ fp => fp.recordId == "RECORD4" && fp.feature.feature.name == "PATH2_VALUE_IS_UNIQUE" }.head.feature.value === BooleanValue(false))
  }

}
