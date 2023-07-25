package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.{HighGrainProfile, LowGrainProfile, OriginalValuePassthrough}
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ExecuteByPathTest extends AnyFunSpec with SparkSessionTestWrapper {

  private def genRecord(id: String, v1: String, v2: String): String =
    s"""{"id":"$id","v1":"$v1","v2":"$v2"}"""

  private val records = Seq(
    genRecord("0", "foo", "bar"),
    genRecord("2", "baz", "bar"),
    genRecord("3", "foo", "foo"),
    genRecord("4", "foo", "baz"),
    genRecord("5", "bar", "baz"),
  )


  it("executes multiple rules and groups the whole output by feature path") {
    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, records, idPath = Option("id"))),
    )

    val paths = Seq("v1", "v2")

    val rules = Seq(
      FieldBasedMask(OriginalValuePassthrough()).withFilterByPaths(paths:_*),
      FieldBasedMask(HighGrainProfile()).withFilterByPaths(paths:_*),
      FieldBasedMask(LowGrainProfile()).withFilterByPaths(paths:_*),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rules:_*
      )

    val ruleCount = rules.size
    val recordCount = records.size
    val pathsCount = paths.size

    val executeMulti = profiledRecords.executeMulti()
    assert(executeMulti.size === ruleCount)

    val executeByPath = profiledRecords.executeByPath()
    assert(executeByPath.size === pathsCount)
    assert(executeByPath.keySet === paths.toSet)

    paths.foreach { path =>
      assert(executeByPath(path).featurePoints.collect().length === ruleCount * recordCount)
    }
  }
}
