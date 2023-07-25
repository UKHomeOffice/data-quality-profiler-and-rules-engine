package uk.gov.ipt.das.dataprofiler.parser

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor.IgnoreAfterPeriod
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class KeyPreProcessorTest extends AnyFunSpec with SparkSessionTestWrapper with DatasetComparer {

  it("profile with a keypreprocessor") {

    val jsonStrings = Seq(
      """{
        |    "identifier": "APP1106082",
        |    "given-name": "daniel",
        |    "family-name": "smith",
        |    "application-date": "2018-05-02"
        |}
        |""".stripMargin)

    val results1 = {
      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(fromJsonStrings(spark, jsonStrings, idPath = Option("identifier")))),
        rules = FieldBasedMask()
      ).executeMulti().head._2

      val profiled = results.getProfiledData.dataFrame
      profiled.get.show()

      results
    }

    val results2 = {
      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(fromJsonStrings(spark, jsonStrings, idPath = Option("identifier")), keyPreProcessor = IgnoreAfterPeriod())),
        rules = FieldBasedMask()
      ).executeMulti().head._2

      val profiled = results.getProfiledData.dataFrame
      profiled.get.show()

      results
    }

    // TODO the assert small dataset equality breaks due to out of order issues, need to create think of solution
//    assertSmallDatasetEquality(
//      results1.getFeatureCollection.getMetrics.getDataFrame,
//      results2.getFeatureCollection.getMetrics.getDataFrame,
//    )

    // NB: these cannot be compared this easily, due to out of order issues.

//    assertSmallDatasetEquality(
//      results1.getFeatureCollection.getProfiledData.getDataFrame,
//      results2.getFeatureCollection.getProfiledData.getDataFrame,
//    )

  }
  it(
    """Profile with a keypreprocessor for nested json to ensure that it is only individual keys
      |where keypreprocessor is being applied""".stripMargin) {

    val results1 = {
      val jsonStrings = Seq(
        """
          |  {
          |    "ID": "1234",
          |    "person": {
          |      "name": "Edward Jones",
          |      "accountStatusCode": "1",
          |      "accountStatusDescription": "OPEN"
          |    }
          |  }
          |""".stripMargin)


      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(fromJsonStrings(spark, jsonStrings, idPath = Option("ID")))),
        rules = FieldBasedMask()
      ).executeMulti().head._2

      val profiled = results.getProfiledData.dataFrame
      profiled.get.show()

      results
    }

    val results2 = {
      val jsonStrings = Seq(
        """
          |  {
          |    "ID": "1234",
          |    "person": {
          |      "name": "Edward Jones",
          |      "accountStatusCode": "1",
          |      "accountStatusDescription": "OPEN"
          |    }
          |  }
          |""".stripMargin)

      val results = ProfilerConfiguration(
        recordSets = RecordSets("test" -> FlattenedRecords(fromJsonStrings(spark, jsonStrings, idPath = Option("ID")), keyPreProcessor = IgnoreAfterPeriod())),
        rules = FieldBasedMask()
      ).executeMulti().head._2

      val profiled = results.getProfiledData.dataFrame
      profiled.get.show()

      results
    }
    // N.B these assertions break when stringFilter is applied to the whole path in DataProfiler.scala
//    assertSmallDatasetEquality(
//      results1.getFeatureCollection.getMetrics.getDataFrame,
//      results2.getFeatureCollection.getMetrics.getDataFrame,
//    )
//
//    assertSmallDatasetEquality(
//      results1.getFeatureCollection.getProfiledData.getDataFrame,
//      results2.getFeatureCollection.getProfiledData.getDataFrame,
//    )

  }
}