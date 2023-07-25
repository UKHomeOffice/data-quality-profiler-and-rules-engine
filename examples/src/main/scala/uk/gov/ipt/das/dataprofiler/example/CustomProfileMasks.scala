package uk.gov.ipt.das.dataprofiler.example

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRowEncoder.{SAMPLE_MIN, _}
import uk.gov.ipt.das.dataprofiler.feature.FeatureDefinition
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.MaskProfiler
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.value.{LongValue, RecordValue}

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

/**
 * Sample application to define a new profile mask and profile data using it
 */
object CustomProfileMasks extends App {

  class CountNumericCharactersMask extends MaskProfiler with Serializable {
    override def profile(value: RecordValue): RecordValue =
      LongValue(value.valueAsString match { case null => 0L case s: String => s.count(c => c.isDigit) })

    override def getDefinition: FeatureDefinition =
      FeatureDefinition("DQ", "NUMERIC_CHAR_COUNT")
  }

  // set up spark
  lazy implicit val spark: SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .master("local[8]")
      .appName("spark session")
      .config("spark.default.parallelism", "8")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("OFF")
    sparkSession
  }

  // load JSON from resource file
  val jsonLines = IOUtils.readLines(getClass.getClassLoader.getResourceAsStream("arxiv/arxiv-top1000.jsonl"), StandardCharsets.UTF_8).asScala

  // parse and flatten JSON into a RecordSets class
  val recordSets = RecordSets("arxiv-top1000" -> FlattenedRecords(fromJsonStrings(spark, jsonLines, idPath = Option("id"))))

  // perform the profiling
  val results =
    ProfilerConfiguration(
      recordSets = recordSets,
      rules = FieldBasedMask(new CountNumericCharactersMask()) // use our new mask defined above
    ).executeMulti()

  // show results
  results.take(1).foreach { arXivResults =>
    arXivResults._2.getMetrics().dataFrame.map { metricsDataframe =>
      println("ArXiv First 1000 records, metrics:")
      metricsDataframe
        .drop(SAMPLE_MIN, SAMPLE_MAX, SAMPLE_FIRST, SAMPLE_LAST) // don't show samples in the summary
        .orderBy(col(FEATURE_PATH).asc, col(COUNT).desc, col(FEATURE_VALUE).asc)
        .show(numRows = 1000, truncate = false)
    }
  }

  // do some analysis on authors_parsed[][]
  results.take(1).foreach { arXivResults =>
    arXivResults._2.getMetrics().dataFrame.map { metricsDataframe =>
      println("ArXiv First 1000 records, samples from authors_parsed[][]:")
      metricsDataframe
        .where(col(FEATURE_PATH) === "authors_parsed[][]")
        .orderBy(col(FEATURE_PATH).asc, col(COUNT).desc, col(FEATURE_VALUE).asc)
        .show(numRows = 1000, truncate = false)
    }
  }

}
