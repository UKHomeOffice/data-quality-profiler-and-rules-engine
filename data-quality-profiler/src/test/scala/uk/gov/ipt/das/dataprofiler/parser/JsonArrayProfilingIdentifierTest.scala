package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.{IdentifierPaths, JsonInputReader}
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

import scala.collection.mutable.ArrayBuffer

class JsonArrayProfilingIdentifierTest extends AnyFunSpec with SparkSessionTestWrapper {

  val json: String =
    s"""{
       | "id": "22",
       | "foo": "bar",
       |  "nest": {"str": "nestedstr"},
       | "recordarray": [
       |   {"substr": "substr1", "substr1": "substr11"},
       |   {"substr": "substr2", "substr1": "substr12"},
       |   {"substr": "substr3", "substr1": "substr13"},
       |   {"record": {"id" : "2"}}
       | ],
       | "nestedarray": [
       |       {"notAnotherNest": [ {"str": "found" }, {"str1": "dontfind" }]},
       |       {"valueToCheck": "exists"}
       | ]
       |}
       |""".stripMargin

  it("looks up jsonpath in a record with array") {

    val record = JsonInputReader.parseString(json)
    println(record.lookup("foo"))
    println(record.lookup("recordarray[].substr1"))
    println(record.lookup("nestedarray[].notAnotherNest[].str"))
    println(record.lookup("recordarray[].record.id"))

    //assertions
    assert(record.lookup("foo").orNull.asString === "bar")
    assert(record.lookup("nest.str").orNull.asString === "nestedstr")
    assert(record.lookup("recordarray[].substr1").orNull.asArray ===
      ArrayBuffer(StringValue("substr11"), StringValue("substr12"), StringValue("substr13")))
    assert(record.lookup("nestedarray[].notAnotherNest[].str").orNull.asArray ===
      ArrayBuffer(StringValue("found")))
    assert(record.lookup("recordarray[].record.id").orNull.asArray ===
      ArrayBuffer(StringValue("2")))
  }

  it("get identifier from array and run profiler config to output dataframe"){
    // read json into profiler config

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(json), idPath = Option("id"),
        identifierPaths = IdentifierPaths(
          "array" -> IdentifierSource.direct("recordarray[].substr1"),
          "errorPath" -> IdentifierSource.direct("recordarray[].notHere"),
          "nestedarray" -> IdentifierSource.direct("nestedarray[].notAnotherNest[].str"))
      ))
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = FieldBasedMask(OriginalValuePassthrough()).withFilterByPaths("foo")
      ).executeMulti().head._2

    val df = profiledRecords.getProfiledData.dataFrame.get
      df.show(false)

    //assertions
    assert(df.select("array").first().getString(0) === "substr11,substr12,substr13")
    assert(df.select("errorPath").first().getString(0) ===
      "_UNKNOWN")
    assert(df.select("nestedarray").first().getString(0) === "found")


  }
  it("value derived json array lookup") {

    val recordStr =
      s"""{
         |  "id": "RECORD0",
         |  "identifier": "information",
         |  "somearray": [
         |    {"valueToProfile": "EDWARD"},
         |    {
         |      "matchme": "matches",
         |      "value": "foo"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux"
         |    }
         |  ],
         |  "valueToProfile": "THOMAS"
         |}
         |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"),
        identifierPaths = IdentifierPaths("identifier" -> IdentifierSource.direct("identifier"))))
        .addValueDerivedArrayIdentifier(
          arrayPath = "somearray",
          valuePath = "somearray[].value",
          lookupPath = "somearray[].matchme",
          lookupValue = "matches",
          identifierName = "matching" )
    )


    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("valueToProfile","somearray[].valueToProfile")
      ).executeMulti().head._2

    val df = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    //assertions
    assert(df.select("matching").first().getString(0) === "foo")
    assert(df.select("identifier").first().getString(0) === "information")

  }

  it("value derived json array lookup throwing value when not found") {

    val recordStr =
      s"""{
         |  "id": "RECORD0",
         |  "identifier": "information",
         |  "somearray": [
         |    {"valueToProfile": "EDWARD"},
         |    {
         |      "nothing": "matches",
         |      "value": "foo"
         |    },
         |    {
         |      "nothing": "DOESNTmatch",
         |      "value": "bar"
         |    },
         |    {
         |      "nothing": "DOESNTmatch",
         |      "value": "guux"
         |    }
         |  ],
         |  "valueToProfile": "THOMAS"
         |}
         |""".stripMargin

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr), idPath = Option("id"),
        identifierPaths = IdentifierPaths("identifier" -> IdentifierSource.direct("identifier"))))
        .addValueDerivedArrayIdentifier(
          arrayPath = "somearray",
          valuePath = "somearray[].value",
          lookupPath = "somearray[].matchme",
          lookupValue = "DONT_FIND_ANYTHING",
          identifierName = "matching" )
    )


    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("valueToProfile","somearray[].valueToProfile")
      ).executeMulti().head._2

    val df = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    //assertions
    assert(df.select("matching").first().getString(0) === "_UNKNOWN")
    assert(df.select("identifier").first().getString(0) === "information")

  }
  it("Deal with nesting and multiple vals") {

    val json2: String =
      s"""{
         | "recordarray": [
         |   {"record": {"id" : {"value": "2"}}},
         |   {"record": {"id" : {"value": "2_OTHER"}}}
         | ]
         |}
         |""".stripMargin

    val record = JsonInputReader.parseString(json2)
    println(record.lookup("recordarray[].record.id.value"))
    assert(record.lookup("recordarray[].record.id.value").toString ===
      "Some(ArrayValue([StringValue(2),StringValue(2_OTHER)]))")
  }
}