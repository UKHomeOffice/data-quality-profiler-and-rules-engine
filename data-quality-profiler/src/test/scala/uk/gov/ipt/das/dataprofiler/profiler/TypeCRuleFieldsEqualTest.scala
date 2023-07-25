package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{JoinedRecordCompareField, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class TypeCRuleFieldsEqualTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("links two records by a join ID and compares if two named path values (fields) are equal") {

    def genRecordA(recordId: String, value: String): String =
      s"""{
         |  "id": "$recordId",
         |  "value": "$value"
         |}
         |""".stripMargin

    def genRecordB(recordId: String, recordAId: String, value: String): String =
      s"""{
         |  "id": "$recordId",
         |  "recordAId": "$recordAId",
         |  "value": "$value"
         |}
         |""".stripMargin

    val recordSetA = List(
      genRecordA(recordId = "RECORD_A_1", value = "Person A 1"),
      genRecordA(recordId = "RECORD_A_2", value = "Person A 2"),
      genRecordA(recordId = "RECORD_A_3", value = "Person A 3"),
    )

    val recordSetB = List(
      genRecordB(recordId = "RECORD_B_1", recordAId = "RECORD_A_1", value = "Person A 1"),
      genRecordB(recordId = "RECORD_B_2", recordAId = "RECORD_A_1", value = "Person A 2"),
      genRecordB(recordId = "RECORD_B_3", recordAId = "RECORD_A_1", value = "Person A 3"),
      genRecordB(recordId = "RECORD_B_4", recordAId = "RECORD_A_2", value = "Person A 1"),
      genRecordB(recordId = "RECORD_B_5", recordAId = "RECORD_A_2", value = "Person A 2"),
      genRecordB(recordId = "RECORD_B_6", recordAId = "RECORD_A_2", value = "Person A 3"),
      genRecordB(recordId = "RECORD_B_7", recordAId = "RECORD_A_3", value = "Person A 1"),
      genRecordB(recordId = "RECORD_B_8", recordAId = "RECORD_A_3", value = "Person A 2"),
      genRecordB(recordId = "RECORD_B_9", recordAId = "RECORD_A_3", value = "Person A 3"),
    )

    val recordSets = RecordSets(
      "a" -> FlattenedRecords(JsonInputReader.fromJsonStrings(spark = spark, jsonStrings = recordSetA, idPath = Option("id"))),
      "b" -> FlattenedRecords(JsonInputReader.fromJsonStrings(spark = spark, jsonStrings = recordSetB, idPath = Option("id"))),
    )

    val results = ProfilerConfiguration(
      recordSets = recordSets,
      rules =
        JoinedRecordCompareField(
          definitionName = "VALUE_FROM_A_MATCHES_VALUE_FROM_B__JOIN_ON_RECORDAID",
          primaryRecordSet = "b",
          joinedRecordSet = "a",
          primaryJoinPath = "recordAId",
          joinedJoinPath = "id",
          primaryComparisonPath = "value",
          joinedComparisonPath = "value"
        )
    ).executeMulti().head._2

    val records = results.featurePoints.collect()

    println(records.mkString("\n"))

    assert(records.length === 9)

    assert(records.filter{ fp => fp.recordId == "RECORD_B_1" }.head.feature.value === BooleanValue(true))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_2" }.head.feature.value === BooleanValue(false))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_3" }.head.feature.value === BooleanValue(false))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_4" }.head.feature.value === BooleanValue(false))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_5" }.head.feature.value === BooleanValue(true))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_6" }.head.feature.value === BooleanValue(false))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_7" }.head.feature.value === BooleanValue(false))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_8" }.head.feature.value === BooleanValue(false))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_9" }.head.feature.value === BooleanValue(true))
  }

}
