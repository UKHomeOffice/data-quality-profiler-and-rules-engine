package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{JoinedRecordExists, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class TypeCRuleReferentialIntegrityTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("links two records by a join ID and compares if two named path values (fields) are equal") {

    def genRecordA(recordId: String): String =
      s"""{
         |  "id": "$recordId"
         |}
         |""".stripMargin

    def genRecordB(recordId: String, recordAId: String): String =
      s"""{
         |  "id": "$recordId",
         |  "recordAId": "$recordAId"
         |}
         |""".stripMargin

    val recordSetA = List(
      genRecordA(recordId = "RECORD_A_1"),
    )

    val recordSetB = List(
      genRecordB(recordId = "RECORD_B_1", recordAId = "RECORD_A_1"),
      genRecordB(recordId = "RECORD_B_2", recordAId = "RECORD_A_4"),
    )

    val recordSets = RecordSets(
      "a" -> FlattenedRecords(JsonInputReader.fromJsonStrings(spark = spark, jsonStrings = recordSetA, idPath = Option("id"))),
      "b" -> FlattenedRecords(JsonInputReader.fromJsonStrings(spark = spark, jsonStrings = recordSetB, idPath = Option("id"))),
    )

    val results = ProfilerConfiguration(
      recordSets = recordSets,
      rules =
        JoinedRecordExists(
          definitionName = "JOIN_ON_RECORDAID__JOINED_RECORD_EXISTS",
          primaryRecordSet = "b",
          joinedRecordSet = "a",
          primaryJoinPath = "recordAId",
          joinedJoinPath = "id"
        )
    ).executeMulti().head._2

    val records = results.featurePoints.collect()

    println(records.mkString("\n"))

    assert(records.length === 2)

    assert(records.filter{ fp => fp.recordId == "RECORD_B_1" }.head.feature.value === BooleanValue(true))
    assert(records.filter{ fp => fp.recordId == "RECORD_B_2" }.head.feature.value === BooleanValue(false))
  }

}
