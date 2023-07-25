package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class FlattenedFeaturePointTest extends AnyFunSpec with SparkSessionTestWrapper{
  it("Test arrays within json string get aggregated to the same field") {
    val jsonStrings = Seq(
      """
        |  {
        |    "ID": "1234",
        |    "name": ["Edward", "Thomas", "Jones"]
        |  }
        |""".stripMargin)

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(fromJsonStrings(spark, jsonStrings, idPath = Option("ID")))),
      rules = FieldBasedMask()
    ).executeMulti().head._2

    val profiled = results.getProfiledData.dataFrame
    val metrics= results.getMetrics().dataFrame
    profiled.get.show(false)
    metrics.get.show(false)

    //assertions
    assert(profiled.get.count() === 8)
    assert(metrics.get.count() === 5)
  }

}
