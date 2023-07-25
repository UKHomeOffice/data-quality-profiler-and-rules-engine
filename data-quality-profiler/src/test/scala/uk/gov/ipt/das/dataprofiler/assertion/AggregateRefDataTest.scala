package uk.gov.ipt.das.dataprofiler.assertion

import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ExcelReferenceDataset
import uk.gov.ipt.das.dataprofiler.dataframe.analysis.RefDataAnalysisDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.ProfiledDataFrame
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{RecordSets, ValueNotPresentInReferenceDataset, ValuePresentInReferenceDataset}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class AggregateRefDataTest extends AnyFunSpec with SparkSessionTestWrapper{
  it("profile the data and then read back in for aggregations on specific values"){
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
      recordFromValue("RECORD2", "A"),
      recordFromValue("RECORD2", "C"),
      recordFromValue("RECORD2", "C"),
      recordFromValue("RECORD2", "B"),
      recordFromValue("RECORD2", "XXXXXX"),
      recordFromValue("RECORD2", "XXXXXX"),
      recordFromValue("RECORD2", "XXXsdasfXXX"),
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

    val profiledData = results.getProfiledData
    profiledData.dataFrame.get.show(truncate = false)

    val finalDf = RefDataAnalysisDataFrame(profiledData).dataFrame.get
    finalDf.show(false)


    println("Running assertions")
    assert(finalDf.count() === 5)
    assert(finalDf.filter(col("originalValue") === "A").select("count").first().getLong(0) === 2)
    assert(finalDf.filter(col("originalValue") === "B").select("count").first().getLong(0) === 1)
    assert(finalDf.filter(col("originalValue") === "XXXXXX").select("count").first().getLong(0) === 2)
  }
  it("test multiple reference data rules"){
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
      recordFromValue("RECORD2", "A"),
      recordFromValue("RECORD2", "C"),
      recordFromValue("RECORD2", "C"),
      recordFromValue("RECORD2", "B"),
      recordFromValue("RECORD2", "XXXXXX"),
      recordFromValue("RECORD2", "XXXXXX"),
      recordFromValue("RECORD2", "XXXsdasfXXX"),
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
          ValueNotPresentInReferenceDataset(
            definitionName = "ref",
            referenceDataset = refData("DS1")
          ).withFilterByPaths("^value$")
      ).executeMulti()

    val profiledDataFrame = results.map{dataFrame => dataFrame._2.getProfiledData.dataFrame.get}.reduce(_.union(_))
    profiledDataFrame.show(truncate = false)

    val profiledData = ProfiledDataFrame(profiledDataFrame)

    val finalDf = RefDataAnalysisDataFrame(profiledData,Seq("DQ_ValuePresentInReferenceDataset","DQ_ref")).dataFrame.get
    finalDf.show(false)


    println("Running assertions")
    assert(finalDf.count() === 10)
    assert(finalDf.filter(col("originalValue") === "A"
      && col("featureName") === "DQ_ref")
      .select("count").first().getLong(0) === 2)
    assert(finalDf.filter(col("originalValue") === "A"
      && col("featureName") === "DQ_ValuePresentInReferenceDataset")
      .select("count").first().getLong(0) === 2)
  }

  it("test filterByPath not working and outputting empty dataframes"){
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
      recordFromValue("RECORD2", "A"),
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
          ).withFilterByPaths("^wrongValue$")
      ).executeMulti().head._2

    val finalDf = RefDataAnalysisDataFrame(results.getProfiledData).dataFrame.get


    println("Running assertions")
    assert(finalDf.count() === 0)
  }

}
