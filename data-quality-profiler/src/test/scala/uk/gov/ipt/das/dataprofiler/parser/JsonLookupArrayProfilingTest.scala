package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.IdentifierPaths
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{ArrayQueryPath, FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class JsonLookupArrayProfilingTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("test profiling value within array based on a lookup and adding query path") {

    val recordStr =
      s"""{
         |  "id": "RECORD0",
         |  "identifier": "information",
         |  "somearray": [
         |    {"name": "EDWARD"},
         |    {
         |      "matchme": "matches",
         |      "value": "foo",
         |      "otherVal": "find"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar",
         |      "otherVal": "dontFind"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux",
         |      "otherVal": "dontFind"
         |    }
         |  ],
         |  "valueToProfile": "THOMAS"
         |}
         |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"),
        identifierPaths = IdentifierPaths("identifier" -> IdentifierSource.direct("identifier")))))

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withArrayQueryPaths(ArrayQueryPath(lookupPath = "somearray[].matchme",finalPath = "somearray[].value"))
      ).executeMulti().head._2

    val df = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    //assertions
    assert(df.select("featurePath").tail(3).head.getString(0) === "somearray[matchme=matches].value")
    assert(df.select("originalValue").tail(3).head.getString(0) === "foo")
    assert(df.select("recordId").tail(3).head.getString(0) === "RECORD0")
    assert(df.select("featurePath").tail(1).last.getString(0) === "somearray[matchme=DOESNTmatch].value")
    assert(df.select("originalValue").tail(1).last.getString(0)  === "guux")
    assert(df.select("recordId").tail(1).last.getString(0)  === "RECORD0")
  }
  it("test profiling value within array based on a lookup and adding query path aswell as other values required") {

    val recordStr =
      s"""{
         |  "id": "RECORD0",
         |  "identifier": "information",
         |  "somearray": [
         |    {"name": "EDWARD"},
         |    {
         |      "matchme": "matches",
         |      "value": "foo",
         |      "otherVal": "find"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar",
         |      "otherVal": "found"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux",
         |      "otherVal": "finder"
         |    }
         |  ],
         |  "valueToProfile": "THOMAS"
         |}
         |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"),
        identifierPaths = IdentifierPaths("identifier" -> IdentifierSource.direct("identifier")))))

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("identifier")
            .withArrayQueryPaths(
              ArrayQueryPath(lookupPath = "somearray[].matchme",finalPath = "somearray[].value"),
              ArrayQueryPath(lookupPath = "somearray[].otherVal",finalPath = "somearray[].value"))
      ).executeMulti().head._2

    val df = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    //assertions
    assert(df.count() === 7)
    assert(df.select("featurePath").first().getString(0) === "identifier")
    assert(df.select("originalValue").first().getString(0) === "information")
    assert(df.select("recordId").first().getString(0) === "RECORD0")
    assert(df.select("featurePath").tail(1).last.getString(0) === "somearray[otherVal=finder].value")
    assert(df.select("originalValue").tail(1).last.getString(0)  === "guux")
    assert(df.select("recordId").tail(1).last.getString(0)  === "RECORD0")
  }
}