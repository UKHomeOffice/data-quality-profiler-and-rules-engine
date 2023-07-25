package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ProfilerConfigurationTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Executes a ProfilerConfiguration") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID_1",
        |  "aString": "some string value",
        |  "aBoolean": true,
        |  "aNumber": 1,
        |  "aDouble": 1.1111,
        |  "aRecord": {
        |    "subStr": "someString",
        |    "subRecord": {
        |      "strValue": "string!",
        |      "strValueAnother": "string two",
        |      "subArr": [
        |        100,
        |        10,
        |        1
        |      ]
        |    }
        |  },
        |  "aArray": [
        |    "arr0",
        |    "arr1",
        |    "arr2"
        |  ]
        |}
        |""".stripMargin

    val jsonRecord2 =
      """{
        |  "id": "SOME_ID_2",
        |  "aString": "some string value",
        |  "aBoolean": true,
        |  "aNumber": 1,
        |  "aDouble": 1.1111,
        |  "aRecord": {
        |    "subStr": "someString",
        |    "subRecord": {
        |      "strValue": "string!",
        |      "strValueAnother": "string two",
        |      "subArr": [
        |        100,
        |        10,
        |        1
        |      ]
        |    }
        |  },
        |  "aArray": [
        |    "arr0",
        |    "arr1",
        |    "arr2"
        |  ]
        |}
        |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"))),
      "testRecords2" -> FlattenedRecords(fromJsonStrings(spark, Seq(jsonRecord2), idPath = Option("id"))),
    )

    val profiledRecords1 =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByRecordsSets("testRecords")
      ).executeMulti().head._2

    val profiledRecords2 =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByRecordsSets("testRecords2")
      ).executeMulti().head._2

    val profiledRecordsBoth =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByRecordsSets("testRecords", "testRecords2")

      ).executeMulti()

    val records1 = profiledRecords1.featurePoints.collect()
    val records2 = profiledRecords2.featurePoints.collect()
    val recordsBoth1 = profiledRecordsBoth.head._2.featurePoints.collect()
    val recordsBoth2 = profiledRecordsBoth(1)._2.featurePoints.collect()

    println(records1.mkString("\n"))
    println()
    println(records2.mkString("\n"))
    println()
    println(recordsBoth1.mkString("\n"))
    println()
    println(recordsBoth2.mkString("\n"))

    assert(records1.length === 14)
    assert(records2.length === 14)
    assert(recordsBoth1.length === 14)
    assert(recordsBoth2.length === 14)

    assert(records1.count { fp => fp.path == "id" } == 1)
    assert(records1.filter{ fp => fp.path == "id" }.head.recordId === "SOME_ID_1")
    assert(records2.count { fp => fp.path == "id" } === 1)
    assert(records2.filter{ fp => fp.path == "id" }.head.recordId === "SOME_ID_2")
    assert(recordsBoth1.count { fp => fp.path == "id" } === 1)
    assert(recordsBoth1.filter{ fp => fp.path == "id" }(0).recordId === "SOME_ID_1")
    assert(recordsBoth2.count { fp => fp.path == "id" } === 1)
    assert(recordsBoth2.filter{ fp => fp.path == "id" }(0).recordId === "SOME_ID_2")

  }
  it("Executes a ProfilerConfiguration with path filters and convert to dataframe") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID_1",
        |  "aString": "some string value",
        |  "aBoolean": true,
        |  "aNumber": 1,
        |  "aDouble": 1.1111,
        |  "aRecord": {
        |    "subStr": "someString",
        |    "subRecord": {
        |      "strValue": "string!",
        |      "strValueAnother": "string two",
        |      "subArr": [
        |        100,
        |        10,
        |        1
        |      ]
        |    }
        |   }
        |}
        |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("aString$", "[.]subStr$")
      ).executeMulti().head._2


    val df = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    assert(df.count() === 2)
    assert(df.select("featurePath").first().getString(0) === "aRecord.subStr"
      || df.select("featurePath").first().getString(0) === "aString")

  }

  it("Repeat to test withFilterByExactPaths") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID_1",
        |  "aString": "some string value",
        |  "aString_type": "NOTTHISONE",
        |  "aBoolean": true,
        |  "aNumber": 1,
        |  "aDouble": 1.1111,
        |  "aRecord": {
        |    "subStr": "someString",
        |    "subRecord": {
        |      "strValue": "string!",
        |      "strValueAnother": "string two",
        |      "subArr": [
        |        100,
        |        10,
        |        1
        |      ]
        |    }
        |   }
        |}
        |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByExactPaths("aString", "aRecord.subStr")
      ).executeMulti().head._2


    val df = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    assert(df.count() === 2)
    assert(df.select("featurePath").first().getString(0) === "aRecord.subStr"
      || df.select("featurePath").first().getString(0) === "aString")

  }

}
