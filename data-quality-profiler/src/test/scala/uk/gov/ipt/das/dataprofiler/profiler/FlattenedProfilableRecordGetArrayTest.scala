package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class FlattenedProfilableRecordGetArrayTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("picks array items out correctly") {

    def genRecord(id: String): String = {
      s"""{
        | "id": "$id",
        | "foo": "bar",
        | "somearray": [
        |   "val0",
        |   "val1",
        |   "val2"
        | ],
        | "someotherarray": [
        |   "anothervalue1",
        |   "anothervalue2",
        |   "anothervalue3"
        | ],
        | "recordarray": [
        |   {"substr": "substr1", "substr1": "substr11"},
        |   {"substr": "substr2", "substr1": "substr12"},
        |   {"substr": "substr3", "substr1": "substr13"}
        | ]
        |}
        |""".stripMargin
    }

    val records = Seq(
      genRecord("RECORD0"),
      genRecord("RECORD1"),
      genRecord("RECORD2"),
    )

    val flatRecords = FlattenedRecords(fromJsonStrings(spark, records, idPath = Option("id")))

    flatRecords.records.collect().map { record =>
      {
        val arr = record.getArray("somearray")
        println(arr.mkString("(", ",", ")"))
        assert(arr.length === 3)

        assert(arr(0).head.recordValue === StringValue("val0"))
        assert(arr(1).head.recordValue === StringValue("val1"))
        assert(arr(2).head.recordValue === StringValue("val2"))
      }

      {
        val arr = record.getArray("recordarray")
        println(arr.mkString("(", ",", ")"))
        assert(arr.length === 3)

        arr(0).find(fv => fv.flatPath == "recordarray[].substr").get.recordValue == StringValue("substr1")
        arr(1).find(fv => fv.flatPath == "recordarray[].substr").get.recordValue == StringValue("substr2")
        arr(2).find(fv => fv.flatPath == "recordarray[].substr").get.recordValue == StringValue("substr3")

        arr(0).find(fv => fv.flatPath == "recordarray[].substr1").get.recordValue == StringValue("substr11")
        arr(1).find(fv => fv.flatPath == "recordarray[].substr1").get.recordValue == StringValue("substr12")
        arr(2).find(fv => fv.flatPath == "recordarray[].substr1").get.recordValue == StringValue("substr13")
      }
    }

  }

}