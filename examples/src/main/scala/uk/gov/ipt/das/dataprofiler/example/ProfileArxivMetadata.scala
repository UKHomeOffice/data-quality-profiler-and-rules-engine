package uk.gov.ipt.das.dataprofiler.example

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.reporting.template.model.{BasicReportTemplate, SourceInfo}
import uk.gov.ipt.das.dataprofiler.reporting.template.{BasicReport, MetricsReport}

import java.awt.Desktop
import java.io.FileWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.LocalDate
import scala.collection.JavaConverters._

/**
 * Sample application to load metadata from ArXiv JSON records, profile it and produce a Data Quality report
 */
object ProfileArxivMetadata extends App {

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
      rules = FieldBasedMask() // uses default masks
    ).executeMulti()

  // loop through the results datasets (there is only 1)
  results.foreach { case (_, profileRecords) =>

    // output the top 1000 lines of the results metrics
    profileRecords.getMetrics().dataFrame.foreach { metricsDataframe =>
      // show some metrics
      metricsDataframe.show(numRows = 1000, truncate = false)
    }

    // create a report and open it using system browser
    MetricsReport.generateReportTemplate(profileRecords.getMetrics()).map { metricsValues =>

      // fill in the template values from the metrics dataframe
      val reportTemplateValues = BasicReportTemplate(
        title = s"ArXiv metadata Data Quality Report",
        date = LocalDate.now().toString,
        sources = SourceInfo.fromRecordSets(recordSets),
        rows = metricsValues.toArray
      )

      // generate the HTML from the built-in template
      val reportStr = BasicReport().generateHTML(reportTemplateValues)

      // write the HTML to a temporary file
      val tmpHtmlFile = Files.createTempFile(s"metrics-report-arxiv", ".html").toFile
      val fw = new FileWriter(tmpHtmlFile)
      fw.write(reportStr)
      fw.close()

      println(s"Written HTML to ${tmpHtmlFile.getAbsolutePath}")

      // open using the system browser
      Desktop.getDesktop.open(tmpHtmlFile)
    }
  }

}
