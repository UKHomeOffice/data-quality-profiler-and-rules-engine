package uk.gov.ipt.das.dataprofiler.parser

import org.apache.commons.lang.StringEscapeUtils
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.PopCheckProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class NullProfilingTest extends AnyFunSpec with SparkSessionTestWrapper {
  it("csv inputs with null values correctly") {

    val testInputCSVs = Map(
      "one-null" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":"foo","field2":"bar"}""")}
           |2,${StringEscapeUtils.escapeCsv("""{"field1":"foo2","field2":null}""")}
           |""".stripMargin,
      "all-null" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":null,"field2":null}""")}
           |2,${StringEscapeUtils.escapeCsv("""{"field1":null,"field2":null}""")}
           |""".stripMargin,
      "null-array" ->
        s"""id,object
           |1,${StringEscapeUtils.escapeCsv("""{"field1":[null,null,null,null],"field2":[null,"foo","bar",null]}""")}
           |""".stripMargin,
    )

    testInputCSVs.foreach{ case (testName, testCSV) =>

      val records = CSVInputReader.readCSVString(spark, Seq(testCSV), "id")

      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(records)),
        rules = FieldBasedMask(PopCheckProfiler())
      ).executeMulti().head._2

      println(testName)
    }
  }

  it("json profile inputs with null values correctly") {
    val testJsonInputs = List(
      """{"id": 1, "field1":"foo2","field2":null}""",
      """{"id": 2, "field1":null,"field2":null}""",
      """{"id": 3, "field1":[null,null,null,null],"field2":[null,"foo","bar",null]}"""
    )

    testJsonInputs.foreach{ testJSON =>

      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(JsonInputReader.fromJsonStrings(spark, Seq(testJSON), idPath = Option("id")))),
        rules = FieldBasedMask(PopCheckProfiler())
      ).executeMulti().head._2

      println(testJSON)
      val metrics = results.getMetrics().dataFrame
      metrics.get.show()
    }
  }
}
