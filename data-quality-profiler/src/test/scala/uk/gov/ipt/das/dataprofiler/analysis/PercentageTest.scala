package uk.gov.ipt.das.dataprofiler.analysis

import org.apache.spark.sql.functions.{bround, col}
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.dataframe.analysis.MetricsPercentDataFrame
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class PercentageTest extends AnyFunSpec with SparkSessionTestWrapper {
    it("Profile three sample companies house records and ensure percentages are as expected") {
      val inputCSV = "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv"
      val ID = " CompanyNumber"

      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(CSVInputReader.readCSV(spark,inputCSV, ID))),
        rules = FieldBasedMask(profilers = MaskProfiler.defaultProfilers: _*)
      ).executeMulti().head._2

      val profiled = results.getProfiledData

      val obj = MetricsPercentDataFrame(profiled, lowerThreshold = 0, upperThreshold = 100)
      val df = obj.dataFrame.get
      val dfCN = df.filter(col("featurePath") === " CompanyNumber")
      dfCN.show(false)

      // assertions based on output
      assert(df.count() === 142)

      assert(dfCN
        .filter(col("featureName") === "DQ_LOWGRAIN" && col("featureValue") === "A9")
        .select(bround(col("percentOccurrence")))
        .first().getDouble(0) === 33.0d)

      assert(dfCN
        .filter(col("featureName") === "DQ_LOWGRAIN" && col("featureValue") === "9")
        .select(bround(col("percentOccurrence")))
        .first().getDouble(0) === 67.0d)

      assert(dfCN
        .filter(col("featureName") === "DQ_HIGHGRAIN" && col("featureValue") === "AA999999")
        .select(col("count"))
        .first().getInt(0) === 1)
    }
  it("Profile three sample companies house records and ensure percentages are as expected with thresholds") {
    val inputCSV = "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv"
    val ID = " CompanyNumber"

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(CSVInputReader.readCSV(spark,inputCSV, ID))),
      rules = FieldBasedMask(profilers = MaskProfiler.defaultProfilers: _*)
    ).executeMulti().head._2

    val profiled = results.getProfiledData

    val obj60 = MetricsPercentDataFrame(profiled, lowerThreshold = 0, upperThreshold = 40)
    val obj60to100 = MetricsPercentDataFrame(profiled, lowerThreshold = 60, upperThreshold = 100)
    val dfUpper60 = obj60.dataFrameWithThresholds.get
    val df60to100 = obj60to100.dataFrameWithThresholds.get

    assert(dfUpper60.count() === 39)
    assert(df60to100.count() === 103)
  }
}
