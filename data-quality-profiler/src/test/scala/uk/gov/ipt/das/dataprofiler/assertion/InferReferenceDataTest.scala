package uk.gov.ipt.das.dataprofiler.assertion

import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ExcelReferenceDataset
import uk.gov.ipt.das.dataprofiler.dataframe.analysis.InferRefDatasetDataFrame
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.OriginalValuePassthrough
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class InferReferenceDataTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("infer what reference data test values should be in based on excel dataset") {

    val srcFile = this.getClass
      .getClassLoader
      .getResourceAsStream("referenceDatasets.xlsx")

    val datasets = ExcelReferenceDataset.fromFile(
      spark = spark,
      srcFile = srcFile,
      worksheetName = "Sheet2",
      datasetColumnNumber = 3,
      valueColumnNumber = 5
    )

    datasets.foreach{ case (key, dataset) =>
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
      recordFromValue("RECORD5", "A"),
      recordFromValue("RECORD3", "G"),
      recordFromValue("RECORD7", "G"),
      recordFromValue("RECORDZ", "Z"),
      recordFromValue("RECORDZ", "Z"),
      recordFromValue("RECORDZ", "Z"),
      recordFromValue("RECORD4", "B"),
      recordFromValue("RECORD3", "D"),
      recordFromValue("RECORD3", "B"),
    )

    val recordSets = RecordSets(
      "test" -> FlattenedRecords(fromJsonStrings(spark, testDataset, idPath = Option("id"))),
    )

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules =
          FieldBasedMask(OriginalValuePassthrough()).withFilterByPaths("value")
      ).executeMulti().head._2


    val df = profiledRecords.getProfiledData
    df.dataFrame.get.show(false)
    val refDataDf = InferRefDatasetDataFrame(dataFrameIn = df, referenceData = datasets,maxRank = 3).dataFrame.get

    refDataDf.show(false)


    //assertions
    assert(refDataDf.filter(col("originalValue") === "A")
      .select("refDataset").first().getString(0) === "DS1")
    assert(refDataDf.filter(col("originalValue") === "A")
      .select("count").first().getLong(0) === 3)
    assert(refDataDf.filter(col("originalValue") === "Z")
      .select("refDataset").first().getString(0) === null)


  }

}
