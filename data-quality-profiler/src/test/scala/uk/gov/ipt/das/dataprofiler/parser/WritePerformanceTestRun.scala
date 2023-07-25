package uk.gov.ipt.das.dataprofiler.parser

import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.input.records.ProfilableRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.wrapper.{FileOutputWrapper, SparkSessionTestWrapper}
import uk.gov.ipt.das.dataprofiler.writer.LocalFileWriter
import uk.gov.ipt.das.mixin.TimerWrapper

import java.io.File

object WritePerformanceTestRun extends App with SparkSessionTestWrapper with FileOutputWrapper with TimerWrapper {

  val inputFilePath = "src/test/resources/parquet-large/"

  def doTest(): Unit = {
    val outputFolder = tmpDir()

    val records = ProfilableRecords(
      spark.read.parquet(inputFilePath)
        .map{ row =>
          JsonInputReader.parseString(row.getString(1)).withId(row.getString(0))
        }
    )

    val pc = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(records)),
      rules = FieldBasedMask()
    )
    val results = pc.executeMulti().head._2

    val writer = LocalFileWriter(folder = outputFolder)

    results.writeAll(writer)

    println(new File(outputFolder).list().mkString("Array(", ", ", ")"))

    val outputs = new File(outputFolder).list()

    // expect 3 total outputs
    assert(outputs.size == 2)

    // expect the three dataframes with schema versions as below:
    val expectedOutputs = Seq(
      "profiler-masks-with-original-data--schemaV4",
      "profiler-masks-with-aggregate-counts-and-samples--schemaV1",
    )
    expectedOutputs.foreach(expectedFilename => assert(outputs.contains(expectedFilename)))

  }

  timer("Json parser") { doTest() }


}
