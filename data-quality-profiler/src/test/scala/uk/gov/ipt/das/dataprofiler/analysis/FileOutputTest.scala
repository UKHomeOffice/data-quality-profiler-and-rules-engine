package uk.gov.ipt.das.dataprofiler.analysis

import com.crealytics.spark.excel.ExcelDataFrameReader
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.dataframe.analysis.MetricsFileOutputDataFrame
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.{FileOutputWrapper, SparkSessionTestWrapper}

import java.io.File

class FileOutputTest extends AnyFunSpec with SparkSessionTestWrapper with FileOutputWrapper{
  it("Test writing a xlsx per column") {
    val inputFilePath = "src/test/resources/test-data/transaction_*"

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromLocation(spark, inputFilePath, Option("when")))),
      rules = FieldBasedMask()
    ).executeMulti().head._2

    val profiledDF = results.getMetrics()
    val csvOutObj = MetricsFileOutputDataFrame(profiledDF)

    profiledDF.dataFrame.get.show(false)

    // writing the profiled dataframe out to multiple excels
    val tempOutput = tmpDir(prefix = "excel_out_location")
    csvOutObj.writeExcelPerField(tempOutput)
    println(s"CSV written to: $tempOutput")
    val excelDf = spark.read.excel(header = true).load(s"$tempOutput/where-DQ_HIGHGRAIN.xlsx")

    // assertions to check the folders output equals number of fields
    assert(Option(new File(tempOutput).list).fold(0)(_.length) === 6)
    assert(excelDf.count() === 2L)
    assert(excelDf.select("isValid").first().getString(0) === null)
  }
  it("Test writing a tsv per column") {
    val inputFilePath = "src/test/resources/test-data/transaction_*"

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromLocation(spark, inputFilePath, Option("when")))),
      rules = FieldBasedMask()
    ).executeMulti().head._2

    val profiledDF = results.getMetrics()
    val csvOutObj = MetricsFileOutputDataFrame(profiledDF)

    profiledDF.dataFrame.get.show(false)

    // writing the profiled dataframe out to multiple tab delimited CSVs
    val tempOutput = tmpDir(prefix = "excel_out_location")
    csvOutObj.writeTSVPerField(tempOutput)
    println(s"CSV written to: $tempOutput")

    // read the tsv back in as a test
    val tsvDf = spark
      .read
      .option("header","true")
      .option("delimiter", "\t")
      .csv(s"$tempOutput/DQ_HIGHGRAIN/featurePath=when/*.csv")

    tsvDf.show()
  }
}
