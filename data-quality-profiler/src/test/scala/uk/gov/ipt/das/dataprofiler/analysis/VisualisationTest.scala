package uk.gov.ipt.das.dataprofiler.analysis

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.{FileOutputWrapper, SparkSessionTestWrapper}

class VisualisationTest extends AnyFunSpec with SparkSessionTestWrapper with FileOutputWrapper{
  it("Generates test JSON") {
    val inputCSV = "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv"
    val ID = " CompanyNumber"

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(CSVInputReader.readCSV(spark,inputCSV, ID))),
      rules = FieldBasedMask()
    ).executeMulti().head._2

    val profiledDF = results.getMetrics().dataFrame.get

    val DFCN = profiledDF
      .filter(col("featurePath") === " CompanyNumber" && col("featureName") === "DQ_HIGHGRAIN")

    val DFCNDropped = DFCN
      .drop(colNames = "featurePath", "featureName", "sampleMin", "sampleMax", "sampleFirst", "sampleLast")

    DFCNDropped.show()

    val tempOutput = tmpDir("company_name_mask_count")
    DFCNDropped.repartition(1).write.mode(SaveMode.Overwrite).csv(tempOutput)
    println(s"CSV written to: $tempOutput")
  }
}