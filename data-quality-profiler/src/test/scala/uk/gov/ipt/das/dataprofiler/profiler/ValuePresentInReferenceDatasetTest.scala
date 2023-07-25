package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{RecordSets, ValuePresentInReferenceDataset}
import uk.gov.ipt.das.dataprofiler.value.BooleanValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ValuePresentInReferenceDatasetTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Executes a ProfilerConfiguration") {

    def templateJsonStr(id: String, value: String): String = {
      s"""{
        |  "id": "$id",
        |  "aString": "$value"
        |}
        |""".stripMargin
    }

    val jsonRecords = Seq(
      templateJsonStr(id = "RECORD0", value = "SW1A 1AA"),
      templateJsonStr(id = "RECORD1", value = "SW1Y 4UR"),
      templateJsonStr(id = "RECORD2", value = "United Kingdom"),
      templateJsonStr(id = "RECORD3", value = "XXXXXX"),
    )

    val recordSets = RecordSets("testRecords" -> FlattenedRecords(fromJsonStrings(spark, jsonRecords, idPath = Option("id"))))

    import spark.implicits._

    val refData = ReferenceDataset(spark.createDataset(Seq(
      "SW1A 1AA",
      "SW1Y 4UR"
    )))

    val profiledRecords1 =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          ValuePresentInReferenceDataset(
            definitionName = "PRESENT_IN_ONS_POSTCODES",
            referenceDataset = refData
          ).withFilterByPaths("^aString$")
      ).executeMulti().head._2

    val records1 = profiledRecords1.featurePoints.collect()

    println(records1.mkString("\n"))

    assert(records1.length === 4)

    assert(records1.filter{ fp => fp.recordId == "RECORD0" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD1" }.head.feature.value === BooleanValue(true))
    assert(records1.filter{ fp => fp.recordId == "RECORD2" }.head.feature.value === BooleanValue(false))
    assert(records1.filter{ fp => fp.recordId == "RECORD3" }.head.feature.value === BooleanValue(false))
  }

}
