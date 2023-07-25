package uk.gov.ipt.das.dataprofiler.analysis

import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollectionEncoder._
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.IdentifierPaths
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class PostProfilingMetricsTest extends AnyFunSpec with SparkSessionTestWrapper {
    it("Profile data then read dataframe back in for metrics grouping as will be done via glue for analysis ") {
      def templateJsonStr(id: String,  value1: String, event: String, date: String): String = {
        s"""{
           |  "id": "$id",
           |  "path": "$value1",
           |  "event" : "$event",
           |  "date": "$date"
           |}
           |""".stripMargin
      }

      val jsonRecords = Seq(
        templateJsonStr(id = "RECORD0",  value1 = "foo",event = "1", date = "2020-03-01" ),
        templateJsonStr(id = "RECORD1",  value1 = "bar", event = "2", date = "2020-02-01"),
        templateJsonStr(id = "RECORD2",  value1 = "fo", event = "2", date = "2020-03-01"),
        templateJsonStr(id = "RECORD3",  value1 = "bar", event = "2", date = "2020-02-01"),
        templateJsonStr(id = "RECORD3",  value1 = "baz", event = "2", date = "2020-02-01"),
        templateJsonStr(id = "RECORD3",  value1 = "foo", event = "2", date = "2020-02-01"),
        templateJsonStr(id = "RECORD3",  value1 = "", event = "2", date = "2020-03-01"),
        templateJsonStr(id = "RECORD3",  value1 = "b", event = "2", date = "2022-02-01"),
        templateJsonStr(id = "RECORD0",  value1 = "bar",event = "1", date = "2020-03-01" ),
      )

      val recordSets = RecordSets(
        "testRecords" -> FlattenedRecords(fromJsonStrings(spark,
          jsonRecords,
          idPath = Option("id"),
          identifierPaths = IdentifierPaths(
            "event" -> IdentifierSource.direct("event"),
            "date" -> IdentifierSource.direct("date")
          )
      )))

      val profiledRecords =
        ProfilerConfiguration(
          recordSets = recordSets,
          rules = FieldBasedMask(profilers = MaskProfiler.defaultProfilers:_*)
            .withFilterByExactPaths("path")
        ).executeMulti().head._2

      profiledRecords.getProfiledData.dataFrame.get.show(false)

      val metricsDf = MetricsDataFrame(Seq("event","date"),
        profiledRecords.getProfiledData.dataFrame)
        .dataFrame
        .get
        .filter(col(FEATURE_NAME) === "DQ_HIGHGRAIN")

      metricsDf.show(false)

      // assertions
      assert(metricsDf.filter(col("count") === 4).select("date").first().getString(0) ===
      "2020-02-01" && metricsDf.filter(col("count") === 4).select("event").first().getString(0) === "2")

      assert(metricsDf.filter(col("featureValue") === "a").select("count").first().getLong(0) === 1)

    }
}
