package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{ArrayRecordMatchRule, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ArrayRecordMatchRuleTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("matches a subarray and runs a named rule on it") {
    val recordStr =
      s"""{
         |  "id": "RECORD0",
         |  "somearray": [
         |    {
         |      "matchme": "matches",
         |      "value": "foo"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar"
         |    },
         |    {
         |      "matchme": "matches",
         |      "value": "quux"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux"
         |    }
         |  ]
         |}
         |""".stripMargin

    val rule = ArrayRecordMatchRule(
      definitionName = "MAXLENGTH3_WHEN_MATCHES",
      arrayPath = "somearray",
      valuePath = "somearray[].value",
      subRecordMatch = {values => values.exists { fv =>
        fv.flatPath == "somearray[].matchme" && fv.recordValue == StringValue("matches")
      }},
      subRuleName = "MaxLength3"
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rule
      ).executeMulti().head._2

    profiledRecords.getProfiledData.dataFrame.get.show(numRows = 1000, truncate = false)

    val records = profiledRecords.featurePoints.collect()

    // one for each passport number found
    assert(records.length === 2)
  }

  it("Another configuration ") {
    val recordStr =
      s"""
         |{
         |  "id": "RECORD1",
         |  "references": [
         |    {
         |      "dataPoint1": {
         |        "uid": "foo",
         |        "visible": "DEFAULT",
         |        "id2": "xxxx-xxxxx-xxxx-xxxx-xxx"
         |      },
         |      "datapoint2": {
         |        "uid": "bar",
         |        "value": "xxxx-xxxx-xxx-xxx-xxxxxxxx",
         |        "id2": "steve"
         |      },
         |      "DATAPOINT": {
         |        "code": "baz",
         |        "value": "THISONE"
         |      },
         |      "referenceValue": "NOT_BLANK_FIELD",
         |      "created": "last week",
         |      "createdBy": null
         |    }
         |  ]
         |}
         |""".stripMargin

    val rule = ArrayRecordMatchRule(
      definitionName = "ArrayRecordNotBlank",
      arrayPath = "references",
      valuePath = "references[].referenceValue",
      subRecordMatch = { values =>
        values.exists { fv =>
          (fv.flatPath == "references[].DATAPOINT.value") &&
            fv.recordValue == StringValue("THISONE")
        }
      },
      subRuleName = "NotBlank"
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rule
      ).executeMulti().head._2

    profiledRecords.getProfiledData.dataFrame.get.show(numRows = 1000, truncate = false)

    val records = profiledRecords.featurePoints.collect()

    // one for each passport number found
    assert(records.length === 1)
  }

  it("Empty References Array configuration ") {
    val recordStr =
      s"""
         |{
         |  "id": "RECORD1",
         |  "notAnArray": "foobar",
         |  "emptyArray" : [],
         |   "references": [
         |    {
         |      "dataPoint1": {
         |        "uid": "foo",
         |        "visible": "DEFAULT",
         |        "id2": "xxxx-xxxxx-xxxx-xxxx-xxx"
         |      },
         |      "datapoint2": {
         |        "uid": "bar",
         |        "value": "xxxx-xxxx-xxx-xxx-xxxxxxxx",
         |        "id2": "steve"
         |      },
         |      "DATAPOINT": {
         |        "code": "baz",
         |        "value": "THISONE"
         |      },
         |      "referenceValue": "NOT_BLANK_FIELD",
         |      "created": "last week",
         |      "createdBy": null
         |    }
         |  ]
         |}
         |""".stripMargin

    val rule = ArrayRecordMatchRule(
      definitionName = "ArrayRecordNotBlank",
      arrayPath = "emptyArray",
      valuePath = "emptyArray[].referenceValue",
      subRecordMatch = { values =>
        values.exists { fv =>
          (fv.flatPath == "emptyArray[].DATAPOINT.value") &&
            fv.recordValue == StringValue("THISONE")
        }
      },
      subRuleName = "NotBlank"
    )

    val ruleTwo = ArrayRecordMatchRule(
      definitionName = "ArrayRecordNotBlank",
      arrayPath = "references",
      valuePath = "references[].referenceValue",
      subRecordMatch = { values =>
        values.exists { fv =>
          (fv.flatPath == "references[].DATAPOINT.value") &&
            fv.recordValue == StringValue("THISONE")
        }
      },
      subRuleName = "NotBlank"
    )

    val ruleThree = ArrayRecordMatchRule(
      definitionName = "ArrayRecordNotBlank",
      arrayPath = "notAnArray",
      valuePath = "notAnArray[].referenceValue",
      subRecordMatch = { values =>
        values.exists { fv =>
          (fv.flatPath == "notAnArray[].DATAPOINT.value") &&
            fv.recordValue == StringValue("THISONE")
        }
      },
      subRuleName = "NotBlank"
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"))),
    )

    println(s"testRecords: ${recordSets.recordSets.values.head.records.collect().mkString}")

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rule, ruleTwo, ruleThree
      ).executeMulti().head._2

    val records = profiledRecords.featurePoints.collect()

    // one for each passport number found
    assert(records.length === 0)

    val profiledRecordsTwo =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rule, ruleTwo, ruleThree
      ).executeMulti()(1)._2

    profiledRecordsTwo.getProfiledData.dataFrame.get.show(numRows = 1000, truncate = false)

    val recordsTwo = profiledRecordsTwo.featurePoints.collect()

    // one for each passport number found
    assert(recordsTwo.length === 1)
  }

}