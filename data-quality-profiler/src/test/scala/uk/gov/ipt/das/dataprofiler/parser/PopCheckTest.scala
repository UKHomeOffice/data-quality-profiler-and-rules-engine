package uk.gov.ipt.das.dataprofiler.parser

import org.apache.commons.lang.StringEscapeUtils
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.PopCheckProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class PopCheckTest extends AnyFunSpec with SparkSessionTestWrapper {
  it("parse JSON files looking at popcheck outputs") {

    val testInputCSVs = Map(
      "all-populated" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":"foo","field2":"bar"}""")}
           |2,${StringEscapeUtils.escapeCsv("""{"field1":"foo2","field2":"bar2"}""")}
           |""".stripMargin,
      "one-null" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":"foo","field2":"bar"}""")}
           |2,${StringEscapeUtils.escapeCsv("""{"field1":"foo2","field2":null}""")}
           |""".stripMargin,
      "one-empty" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":"foo","field2":"bar"}""")}
           |2,${StringEscapeUtils.escapeCsv("""{"field1":"foo2","field2":""}""")}
           |""".stripMargin,
      "one-missing" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":"foo","field2":"bar"}""")}
           |2,${StringEscapeUtils.escapeCsv("""{"field1":"foo2"}""")}
           |""".stripMargin
    )

    testInputCSVs.foreach { case (testName, testCSV) =>

      val records = CSVInputReader.readCSVString(spark, Seq(testCSV), "id")

      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(records)),
        rules = PopCheckProfiler().asMask
      ).executeMulti().head._2

      println(testName)
    }
  }

  it ("parse companies house JSON files looking at popcheck outputs") {

    import spark.implicits._

    val sparkRecords = CSVInputReader.readCSV(
      spark = spark,
      path = "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv",
      idColumn = " CompanyNumber"
    )

    sparkRecords.records.map { record => record.getEntries.mkString("\n") }
      .collect().foreach{ println }

    val results = ProfilerConfiguration(
      recordSets = RecordSets("test" -> FlattenedRecords(sparkRecords)),
      rules = FieldBasedMask(PopCheckProfiler())
    ).executeMulti().head._2

    val metrics = results.getProfiledData.dataFrame
    metrics.get.show(numRows = 1000, truncate = false)

  }
}