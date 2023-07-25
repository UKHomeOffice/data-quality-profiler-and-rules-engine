package uk.gov.ipt.das.dataprofiler.parser

import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.{IdentifierPaths, JsonInputReader}
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.{HighGrainProfile, OriginalValuePassthrough}
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class YearMonthIdentifierTests extends AnyFunSpec with SparkSessionTestWrapper {

  it("Resolve additional identifiers and convert timestamp identifiers to year month format") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID_1",
        |  "aString": "some string value",
        |  "anotherString": "some string value 2",
        |  "created": "2020-08-05T07:10:25.439"
        |}
        |""".stripMargin

    val recordSet = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"),
        identifierPaths = IdentifierPaths("created" -> IdentifierSource("created", "TimestampToYearMonth")))))

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSet,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("aString", "anotherString")
      ).executeMulti().head._2

    val df  = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    //assertions
    assert(df.select("created").first().getString(0) === "2020-08")
  }

  it("gets metrics also grouped by an additional identifier for timestamp converted to year month") {
    val jsonStrings = Seq(
      """{
        |  "id": "RECORD0",
        |  "value": "foo",
        |  "group": "group1",
        |  "created": "2020-08-05T10:10:25.439"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD1",
        |  "value": "bar",
        |  "group": "group1",
        |  "created": "2021-09-05T07:25:25.439"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD2",
        |  "value": "foo",
        |  "group": "group2",
        |  "created": "2022-08-05T07:10:25.439"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD3",
        |  "value": "qux",
        |  "group": "group2",
        |  "created": "2020-08-05T07:10:25.439"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD4",
        |  "value": "quxx",
        |  "group": "group2",
        |  "created": "2021-01-05T07:10:25.439"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD4",
        |  "value": "qux",
        |  "group": "group2",
        |  "created": "2020-08-05T07:10:25.439"
        |}
        |""".stripMargin,
    )

    val identifierPaths = IdentifierPaths(
      "group" -> IdentifierSource.direct("group"),
      "created" -> IdentifierSource("created", "TimestampToYearMonth")
    )

    val records = JsonInputReader.fromJsonStrings(
      spark = spark,
      jsonStrings = jsonStrings,
      idPath = Option("id"),
      identifierPaths = identifierPaths
    )

    val recordSets = RecordSets(
      "testSet" -> FlattenedRecords(records)
    )

    val results = ProfilerConfiguration(
      recordSets = recordSets,
      rules = FieldBasedMask(HighGrainProfile()).withFilterByPaths("value")
    ).executeMulti().head._2

    val metrics = results
      .getMetrics("group","created")
      .dataFrame
      .get

    metrics.show(numRows = 1000, truncate = false)

    //assertions
    assert(metrics.filter(col("sampleMin") === "qux")
      .select("count").first().getLong(0) === 2 && metrics.filter(col("sampleMin") === "qux")
      .select("group").first().getString(0) === "group2")
    assert(metrics.filter(col("sampleMin") === "bar")
      .select("count").first().getLong(0) === 1 && metrics.filter(col("sampleMin") === "bar")
      .select("group").first().getString(0) === "group1")
  }

  it("Test parsing funky input timestamp identifier") {
    val jsonRecord =
      """{
        |  "id": "SOME_ID_1",
        |  "aString": "some string value",
        |  "anotherString": "some string value 2",
        |  "created": "2020-08-05ERROR07:10ERROR25.439"
        |}
        |""".stripMargin

    val recordSet = RecordSets(
      "testRecords" -> FlattenedRecords(fromJsonStrings(spark, Seq(jsonRecord), idPath = Option("id"),
        identifierPaths = IdentifierPaths("created" -> IdentifierSource("created", "TimestampToYearMonth")))))

    val profiledRecords =
      ProfilerConfiguration(
        recordSets = recordSet,
        rules =
          FieldBasedMask(OriginalValuePassthrough())
            .withFilterByPaths("aString", "anotherString")
      ).executeMulti().head._2

    val df  = profiledRecords.getProfiledData.dataFrame.get
    df.show(false)

    //assertions
    assert(df.select("created").first().getString(0) === "INVALID_DATETIME")
  }

}
