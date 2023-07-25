package uk.gov.ipt.das.dataprofiler.assertion

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ExcelReferenceDataset
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{ArrayRecordNotPresentInReferenceDataset, ArrayRecordPresentInReferenceDataset, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.StringValue
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class ArrayRecordPresentInReferenceDatasetTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("matches a subarray and runs a reference dataset check value") {
    val recordStr0 =
      s"""{
         |  "id": "RECORD0",
         |  "somearray": [
         |    {
         |      "matchme": "notMatches",
         |      "value": "A"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar"
         |    },
         |    {
         |      "matchme": "matches",
         |      "value": "X"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux"
         |    }
         |  ]
         |}
         |""".stripMargin

    val recordStr1 =
      s"""
        |{
        |  "id": "RECORD1",
        |  "somearray": [
        |    {
        |      "matchme": "NotMatches",
        |      "value": "A"
        |    },
        |    {
        |      "matchme": "DOESNTmatch",
        |      "value": "bar"
        |    },
        |    {
        |      "matchme": "matches",
        |      "value": "A"
        |    },
        |    {
        |      "matchme": "DOESNTmatch",
        |      "value": "guux"
        |    }
        |  ]
        |}
        |""".stripMargin

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

    val rule1 = ArrayRecordPresentInReferenceDataset(
      definitionName = "ArrayRecordPresentInReferenceDataset",
      arrayPath = "somearray",
      valuePath = "somearray[].value",
      subRecordMatch = {values => values.exists { fv =>
        fv.flatPath == "somearray[].matchme" && fv.recordValue == StringValue("matches")
      }},
      referenceDataset = refData("DS1")
    )
    val rule2 = ArrayRecordPresentInReferenceDataset( // add second rule to check refData.AsSet is reusing loaded data correctly
      definitionName = "ArrayRecordPresentInReferenceDataset",
      arrayPath = "somearray",
      valuePath = "somearray[].value",
      subRecordMatch = {values => values.exists { fv =>
        fv.flatPath == "somearray[].matchme" && fv.recordValue == StringValue("matches")
      }},
      referenceDataset = refData("DS1")
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr0, recordStr1), idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rule1, rule2
      ).executeMulti().head._2

    // generate+show metrics for reference
    profiledRecords.getMetrics().dataFrame.get.show(numRows = 1000, truncate = false)

    // assert that assertions are as expected
    println("Starting assertions")
    val profiledData = profiledRecords.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val assertionsView = datasetView(profiledData.get)

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD0' AND
         | featureName = 'DQ_ArrayRecordPresentInReferenceDataset'""".stripMargin).first().getString(0) === "false")
    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD1' AND
         | featureName = 'DQ_ArrayRecordPresentInReferenceDataset'""".stripMargin).first().getString(0) === "true")

  }
  it("matches a subarray and runs a 'not in' reference dataset check on value ") {
    val recordStr0 =
      s"""{
         |  "id": "RECORD0",
         |  "somearray": [
         |    {
         |      "matchme": "notMatches",
         |      "value": "A"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar"
         |    },
         |    {
         |      "matchme": "matches",
         |      "value": "X"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux"
         |    }
         |  ]
         |}
         |""".stripMargin

    val recordStr1 =
      s"""
         |{
         |  "id": "RECORD1",
         |  "somearray": [
         |    {
         |      "matchme": "NotMatches",
         |      "value": "A"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "bar"
         |    },
         |    {
         |      "matchme": "matches",
         |      "value": "A"
         |    },
         |    {
         |      "matchme": "DOESNTmatch",
         |      "value": "guux"
         |    }
         |  ]
         |}
         |""".stripMargin

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


    val rule = ArrayRecordNotPresentInReferenceDataset(
      definitionName = "ArrayRecordPresentInReferenceDataset",
      arrayPath = "somearray",
      valuePath = "somearray[].value",
      subRecordMatch = {values => values.exists { fv =>
        fv.flatPath == "somearray[].matchme" && fv.recordValue == StringValue("matches")
      }},
      referenceDataset = refData("DS1")
    )

    val recordSets = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(recordStr0, recordStr1), idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = rule
      ).executeMulti().head._2

    // generate+show metrics for reference
    profiledRecords.getMetrics().dataFrame.get.show(numRows = 1000, truncate = false)

    // assert that assertions are as expected
    println("Starting assertions")
    val profiledData = profiledRecords.getProfiledData.dataFrame
    profiledData.get.show(numRows = 1000, truncate = false)

    val assertionsView = datasetView(profiledData.get)

    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD0' AND
         | featureName = 'DQ_ArrayRecordPresentInReferenceDataset'""".stripMargin).first().getString(0) === "true")
    assert(spark.sql(
      s"""SELECT featureValue FROM $assertionsView WHERE recordId = 'RECORD1' AND
         | featureName = 'DQ_ArrayRecordPresentInReferenceDataset'""".stripMargin).first().getString(0) === "false")

  }

}