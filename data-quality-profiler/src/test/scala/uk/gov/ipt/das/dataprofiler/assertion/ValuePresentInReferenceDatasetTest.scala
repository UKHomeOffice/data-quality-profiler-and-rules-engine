package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ExcelReferenceDataset
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{RecordSets, ValueNotPresentInReferenceDataset, ValuePresentInReferenceDataset}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ValuePresentInReferenceDatasetTest extends AnyFunSpec with SparkSessionTestWrapper{

  it("Validate ValuePresentInReferenceDataset") {

    val srcFile = this.getClass
      .getClassLoader
      .getResourceAsStream("referenceDatasets.xlsx")

    val refData = ExcelReferenceDataset.fromFile(
      spark = spark,
      srcFile = srcFile,
      worksheetName = "Sheet2",
      datasetColumnNumber = 3,
      valueColumnNumber = 5
    )

    refData.foreach{ case (key, dataset) =>
      println(s"$key -> ${dataset.df.collect().mkString("Array(", ", ", ")")}")
    }

    def recordFromValue(id: String, value: String): String = {
      val wrappedStr =
        if (value == null) {
          "null"
        } else {
          "\"" + value + "\""
        }

      s"""{
         | "id": "$id",
         | "value": $wrappedStr
         |}
         |""".stripMargin
    }

    val testDataset = Seq(
      recordFromValue("RECORD1", "A"),
      recordFromValue("RECORD2", "XXXXXX"),
    )

    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          ValuePresentInReferenceDataset(
            definitionName = "ValuePresentInReferenceDataset",
            referenceDataset = refData("DS1")
            ).withFilterByPaths("^value$"),
          ValuePresentInReferenceDataset(
            definitionName = "ValuePresentInReferenceDataset",
            referenceDataset = refData("DS1")
          ).withFilterByPaths("^value$")
      ).executeMulti().head._2


    // generate+show metrics for reference
    results.getMetrics().dataFrame.get.show(numRows = 1000, truncate = false)

    // assert that assertions are as expected
    println("Starting assertions")
    val profiledData = results.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val assertionsView = datasetView(profiledData.get)

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD1' AND
         | featureName = 'DQ_ValuePresentInReferenceDataset'""".stripMargin).first().getString(0) === "true")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD2' AND
         | featureName = 'DQ_ValuePresentInReferenceDataset'""".stripMargin).first().getString(0) === "false")

  }
  it("Validate ValueNotPresentInReferenceDataset") {

    val srcFile = this.getClass
      .getClassLoader
      .getResourceAsStream("referenceDatasets.xlsx")

    val refData = ExcelReferenceDataset.fromFile(
      spark = spark,
      srcFile = srcFile,
      worksheetName = "Sheet2",
      datasetColumnNumber = 3,
      valueColumnNumber = 5
    )

    refData.foreach{ case (key, dataset) =>
      println(s"$key -> ${dataset.df.collect().mkString("Array(", ", ", ")")}")
    }

    def recordFromValue(id: String, value: String): String = {
      val wrappedStr =
        if (value == null) {
          "null"
        } else {
          "\"" + value + "\""
        }

      s"""{
         | "id": "$id",
         | "value": $wrappedStr
         |}
         |""".stripMargin
    }

    val testDataset = Seq(
      recordFromValue("RECORD1", "A"),
      recordFromValue("RECORD2", "XXXXXX"),
    )

    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          ValueNotPresentInReferenceDataset(
            definitionName = "ValueNotPresentInReferenceDataset",
            referenceDataset = refData("DS1")
          ).withFilterByPaths("^value$"),
          ValueNotPresentInReferenceDataset(
            definitionName = "ValueNotPresentInReferenceDataset",
            referenceDataset = refData("DS1")
          ).withFilterByPaths("^value$")
      ).executeMulti().head._2


    // generate+show metrics for reference
    results.getMetrics().dataFrame.get.show(numRows = 1000, truncate = false)

    // assert that assertions are as expected
    println("Starting assertions")
    val profiledData = results.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val assertionsView = datasetView(profiledData.get)

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD1' AND
         | featureName = 'DQ_ValueNotPresentInReferenceDataset'""".stripMargin).first().getString(0) === "false")

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD2' AND
         | featureName = 'DQ_ValueNotPresentInReferenceDataset'""".stripMargin).first().getString(0) === "true")

  }
}
