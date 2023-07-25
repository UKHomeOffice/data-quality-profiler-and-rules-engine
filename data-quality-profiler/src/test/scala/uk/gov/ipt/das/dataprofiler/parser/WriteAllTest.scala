package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.{FileOutputWrapper, SparkSessionTestWrapper}
import uk.gov.ipt.das.dataprofiler.writer.LocalFileWriter
import uk.gov.ipt.das.mixin.TimerWrapper

import java.io.File

class WriteAllTest extends AnyFunSpec with SparkSessionTestWrapper with FileOutputWrapper with TimerWrapper {
  it("write all outputs of profiler to a single sub-directory using writeAll call") {
    val inputFilePath = "src/test/resources/test-data/simple.json"
    val outputFolder = tmpDir()

    val writer = LocalFileWriter(folder = outputFolder)

    val pc = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromLocation(spark, inputFilePath, idPath = Option("topLevelString")))),
      rules = FieldBasedMask()
    )
    val results = pc.executeMulti().head._2
    results.writeAll(writer)

    println(new File(outputFolder).list().mkString("Array(", ", ", ")"))

    val outputs = new File(outputFolder).list()

    // expect four total outputs
    assert(outputs.size === 2)

    // expect the three dataframes with schema versions as below:
    val expectedOutputs = Seq(
      "profiler-masks-with-original-data--schemaV4",
      "profiler-masks-with-aggregate-counts-and-samples--schemaV2",
    )
    expectedOutputs.foreach(expectedFilename => assert(outputs.contains(expectedFilename)))

    println(s"Named files written: ${writer.getNamedFilenames.mkString("(", ",", ")")}")
    println(s"Dataframes written: ${writer.getDataFrameLocations.mkString("(", ",", ")")}")
  }

}
