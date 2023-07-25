package uk.gov.ipt.das.dataprofiler.profiler

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.parser.WritePerformanceTestRun.{deleteFile, tmpDir}
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper
import uk.gov.ipt.das.dataprofiler.writer.JsonInjectedDQWriter

class CompaniesHouseJsonOutputTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("Executes a ProfilerConfiguration and output masks injected into JSON") {
    val recordSets = RecordSets(
      "input" -> FlattenedRecords(CSVInputReader.readCSV(spark,
        "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv",
        idColumn = "CompanyName"))
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = FieldBasedMask()
      ).executeMulti().head._2

    results.featurePoints.collect()

    results.asJSONStrings(pretty = true).show(false)
  }

  it("Executes a ProfilerConfiguration and outputs Json with masks injected") {
    val recordSets = RecordSets(
      "input" -> FlattenedRecords(CSVInputReader.readCSV(spark,
        "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv",
        idColumn = "CompanyName"))
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = FieldBasedMask()
      ).executeMulti()

    val outputDir = tmpDir("test-tmp-json")

    deleteFile(outputDir)

    results.foreach { case (resultName, result) =>
      JsonInjectedDQWriter(result, sparkSession = spark, path = outputDir).writeToJson()
    }

    spark.read.option("wholeFile", value = true).option("mode", "PERMISSIVE").json(outputDir).show(false)


  }

}
