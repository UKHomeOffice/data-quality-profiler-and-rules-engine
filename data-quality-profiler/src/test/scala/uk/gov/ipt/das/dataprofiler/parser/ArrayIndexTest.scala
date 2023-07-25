package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ArrayIndexTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Creates JSON path array indexes correctly") {

    val jsonStrings = Seq(
      """{
        |  "id": "DOCID0",
        |  "anArray": [
        |    "string0",
        |    "string1",
        |    "string2"
        |  ]
        |}
        |""".stripMargin)

    val records = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromJsonStrings(spark, jsonStrings, idPath = Option("id")))),
      rules = FieldBasedMask(OriginalValuePassthrough())
    ).executeMulti().head._2

    val featurePoints = records
      .featurePoints
      .collect()
      .filter{ _.path != "id" }

    List(0,1,2).map { index =>
//      assert(featurePoints(index).path === s"anArray[$index]") // values are without indices now
      assert(featurePoints(index).path === s"anArray[]")
      assert(featurePoints(index).feature.getValueAsString === s"string$index")
    }
  }

}
