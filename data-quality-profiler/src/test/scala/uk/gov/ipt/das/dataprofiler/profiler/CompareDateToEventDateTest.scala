package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{Comparator, CompareDateToEventDate, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.Comparator.{AFTER, BEFORE}
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class CompareDateToEventDateTest extends AnyFunSpec with SparkSessionTestWrapper {

  def templateJsonStr(id: String, v1: String, v2: String): String = {
    s"""{
       |  "id": "$id",
       |  "v1": "$v1",
       |  "v2": "$v2"
       |}
       |""".stripMargin
  }

  private val jsonRecords = Seq(
    templateJsonStr(id = "RECORD0", v1 = "2020-01-01T00:00:00Z", v2 = "2019-01-01T00:00:00Z"), // v2 is before
    templateJsonStr(id = "RECORD1", v1 = "2020-01-01T00:00:00Z", v2 = "2020-01-01T00:00:00Z"), // v2 is same
    templateJsonStr(id = "RECORD2", v1 = "2020-01-01T00:00:00Z", v2 = "2021-01-01T00:00:00Z"), // v2 is after
    templateJsonStr(id = "RECORD3", v1 = "2020-01-01", v2 = "2019-01-01T00:00:00Z"), // v2 is before
    templateJsonStr(id = "RECORD4", v1 = "2020-01-01", v2 = "2019-01-01T00:00:202"), // v2 is before
    templateJsonStr(id = "RECORD5", v1 = "2020-03-03T07:15:2Z", v2 = "2020-01-01"), // v2 is before
//    2020-05-21T07:15:2Z

  )

  private val recordSets = RecordSets(
    "testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"))),
  )

  it("compares dates are before other date fields") {
    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompareDateToEventDate(
            definitionName = "V2_BEFORE_V1",
            datePath = "v2",
            comparator = BEFORE,
            eventDatePath = "v1"
          )
      ).executeMulti().head._2

    val records = profiledRecords.featurePoints.collect()

    println(records.mkString("\n"))

    assert(records.length === 6)

    assert(records.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === StringValue("true"))
    assert(records.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === StringValue("false"))
    assert(records.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === StringValue("false"))
    assert(records.filter{ fp => fp.recordId == "RECORD3" }.head.feature.value === StringValue("true"))
    assert(records.filter{ fp => fp.recordId == "RECORD4" }.head.feature.value === StringValue("true"))
    assert(records.filter{ fp => fp.recordId == "RECORD5" }.head.feature.value === StringValue("true"))

  }

  it("compares dates are after other date fields") {
    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          CompareDateToEventDate(
            definitionName = "V2_AFTER_V1",
            datePath = "v2",
            comparator = AFTER,
            eventDatePath = "v1"
          )
      ).executeMulti().head._2

    val records = profiledRecords.featurePoints.collect()

    println(records.mkString("\n"))

    assert(records.length === 6)

    assert(records.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === StringValue("false"))
    assert(records.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === StringValue("false"))
    assert(records.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === StringValue("true"))
    assert(records.filter{ fp => fp.recordId == "RECORD3" }.head.feature.value === StringValue("false"))
    assert(records.filter{ fp => fp.recordId == "RECORD4" }.head.feature.value === StringValue("false"))
    assert(records.filter{ fp => fp.recordId == "RECORD5" }.head.feature.value === StringValue("false"))

  }

}
