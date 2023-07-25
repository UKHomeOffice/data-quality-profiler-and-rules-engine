package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.{FlatValue, FlattenedRecords}
import uk.gov.ipt.das.dataprofiler.value.{BooleanValue, DoubleValue, LongValue, StringValue}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

import java.util.regex.Pattern

class RecordFlattenerTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Flattens a record soundly and completely") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID",
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
        |  ],
        |  "aArrayOfRecords": [
        |    {"str": "hello0"},
        |    {"str": "hello1"},
        |    {"str": "hello2"}
        |  ]
        |}
        |""".stripMargin

    val record = fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"))

    val flattened = FlattenedRecords(record)

    val flattenedCollected = flattened.records.collect()

    assert(flattenedCollected.length == 1)

    val flattenedRecord = flattenedCollected.head
    val flatValues = flattenedRecord.flatValues.toList

    assert(flatValues(0) === FlatValue("id","id",StringValue("SOME_ID")))
    assert(flatValues(1) === FlatValue("aString","aString",StringValue("some string value")))
    assert(flatValues(2) === FlatValue("aBoolean","aBoolean",BooleanValue(true)))
    assert(flatValues(3) === FlatValue("aNumber","aNumber",LongValue(1)))
    assert(flatValues(4) === FlatValue("aDouble","aDouble",DoubleValue(1.1111)))
    assert(flatValues(5) === FlatValue("aRecord.subStr","aRecord.subStr",StringValue("someString")))
    assert(flatValues(6) === FlatValue("aRecord.subRecord.strValue","aRecord.subRecord.strValue",StringValue("string!")))
    assert(flatValues(7) === FlatValue("aRecord.subRecord.strValueAnother","aRecord.subRecord.strValueAnother",StringValue("string two")))
    assert(flatValues(8) === FlatValue("aRecord.subRecord.subArr[]","aRecord.subRecord.subArr[0]",LongValue(100))) // without indices now
    assert(flatValues(9) === FlatValue("aRecord.subRecord.subArr[]","aRecord.subRecord.subArr[1]",LongValue(10))) // without indices now
    assert(flatValues(10) === FlatValue("aRecord.subRecord.subArr[]","aRecord.subRecord.subArr[2]",LongValue(1))) // without indices now
    assert(flatValues(11) === FlatValue("aArray[]","aArray[0]",StringValue("arr0"))) // without indices now
    assert(flatValues(12) === FlatValue("aArray[]","aArray[1]",StringValue("arr1"))) // without indices now
    assert(flatValues(13) === FlatValue("aArray[]","aArray[2]",StringValue("arr2"))) // without indices now
    assert(flatValues(14) === FlatValue("aArrayOfRecords[].str","aArrayOfRecords[0].str",StringValue("hello0"))) // without indices now
    assert(flatValues(15) === FlatValue("aArrayOfRecords[].str","aArrayOfRecords[1].str",StringValue("hello1"))) // without indices now
    assert(flatValues(16) === FlatValue("aArrayOfRecords[].str","aArrayOfRecords[2].str",StringValue("hello2"))) // without indices now
  }

  it("Flattens a record and applies filter paths") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID",
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
        |  ],
        |  "aArrayOfRecords": [
        |    {"str": "hello0"},
        |    {"str": "hello1"},
        |    {"str": "hello2"}
        |  ]
        |}
        |""".stripMargin

    val record = fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"))

    val flattened = FlattenedRecords(record)

    val filterFlat = flattened.
      filterByPaths(Option(List(Pattern.compile("[.]subStr$"))))

    println(filterFlat.records.collect().mkString)
    val filterRecords = filterFlat.records.head
    val FlatValues = filterRecords.flatValues.toList
    println(FlatValues)

    //assertions
    assert(filterFlat.records.collect().length == 1)
    assert(FlatValues.head === FlatValue("aRecord.subStr","aRecord.subStr",StringValue("someString")))
  }

  it("Checks when no recordId is found that UNKNOWN is defaulted") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID",
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
        |  ],
        |  "aArrayOfRecords": [
        |    {"str": "hello0"},
        |    {"str": "hello1"},
        |    {"str": "hello2"}
        |  ]
        |}
        |""".stripMargin

    val record = fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("NOTHING"))
    val flattened = FlattenedRecords(record)
    println(flattened.records.head.id)

    //assertions
    assert(flattened.records.head.id === "_UNKNOWN")

  }

}
