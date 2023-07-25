package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.{FileOutputWrapper, SparkSessionTestWrapper}
import uk.gov.ipt.das.dataprofiler.writer.StringWriter

class SparkRowJSONProfilerTest extends AnyFunSpec with SparkSessionTestWrapper with FileOutputWrapper {
  it("parse CSV string, write out to String") {

    val csvInputString =
      """id,name,meta,doubles,bools
        |1,dan,ok,1.1,true
        |2,,anull,1.2,false
        |3,somethingelse,ok,1.3,true
        |""".stripMargin.lines.toList

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(CSVInputReader.readCSVString(spark = spark, csvStrings = csvInputString, idColumn = "id"))),
      rules = FieldBasedMask()
    ).executeMulti().head._2

    val stringWriter = StringWriter()
    results.writeAll(stringWriter)

    assert(stringWriter.getLastDataFrame.isDefined)
  }


  it("parse JSON file to JSON, injecting profiler masks where possible, no filtering") {

    val inputFilePath = "src/test/resources/test-data/simple.json"

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromLocation(spark, inputFilePath, Option("topLevelString")))),
      rules = FieldBasedMask()
    ).executeMulti().head._2

    // single file input, so each count must be equal to 1 or 2 for arrays that are aggregating together
    results.getReportRows.foreach(println)
    results.getReportRows.map{ featureReportRow =>
      assert(featureReportRow.count === 1 || featureReportRow.count === 2 || featureReportRow.count === 4)
    }

    // we are expecting no errors, specifics are handled in other tests
  }

  it("Run the Spark profiler on json files locally and show the values in the DataFrame are as expected") {

    val inputFilePath = "src/test/resources/test-data/transaction_*"

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromLocation(spark, inputFilePath, Option("when")))),
      rules = FieldBasedMask(profilers = MaskProfiler.defaultProfilers: _*)
    ).executeMulti().head._2

    val df = results.getMetrics().dataFrame.get
    df.show()

    df.createOrReplaceTempView("Metrics")
    // sql queries to then write assertions against
    val highGrain = spark.sql(
      "select featureValue from Metrics where featurePath = 'when' and featureName = 'DQ_HIGHGRAIN'")
    val lowGrain = spark.sql(
      "select featureValue from Metrics where featurePath = 'when' and featureName = 'DQ_LOWGRAIN'")

    // assertions based on the queries
    lowGrain.show(false)
    assert(highGrain.first().getString(0) === "9999-99-99A99:99:99A")
    assert(lowGrain.first().getString(0) === "9-9-9A9:9:9A")
    // check the counts are consistent
    println(df.count())
    assert(df.count() === 8)
  }

}
