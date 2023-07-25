package uk.gov.ipt.das.dataprofiler.reporting.template

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader.fromJsonStrings
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.csv.CSVInputReader
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.IdentifierPaths
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.{BuiltInFunction, MaskProfiler, OriginalValuePassthrough}
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.reporting.template.model.BasicReportTemplate
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

import java.io.{File, FileOutputStream, FileWriter}
import java.nio.file.Files
import java.time.LocalDate
import scala.util.Random

class MetricsReportTest extends AnyFunSpec with SparkSessionTestWrapper {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private val profilers: Seq[MaskProfiler] =
    MaskProfiler.defaultProfilers ++
      Seq(
        OriginalValuePassthrough(),
        BuiltInFunction(name = "NotBlank"),
        BuiltInFunction(name = "DateAfter19900101"),
        BuiltInFunction(name = "DateTimeAfter19900101"),
        BuiltInFunction(name = "IntegerAtLeast10"),
        BuiltInFunction(name = "ValidTimestamp"),
        BuiltInFunction(name = "ValidYear"),
        BuiltInFunction(name = "OnlyPermittedCharacters-allAlphaNumeric")
      )


  private val metricsDataFrame: MetricsDataFrame = {

    def templateJsonStr(id: String, name: String, town: String, country: String): String = {
      val townStr = if (town == null) "" else "\"town\": \"" + town+ "\","
      val countryStr = if (country == null) "" else "\"country\": \"" + country + "\","
      s"""{ "id": "$id",
          |  $townStr
          |  $countryStr
          |  "name": "$name"
          |}
         |""".stripMargin
    }

    def genRecords(names: String*): Seq[String] = {
      var id = 3
      names.flatMap{ name =>
        id += 2
        (1 to id).map { i =>
          val town = if (Random.nextInt(100) < 20) "London" else null
          val country = if (Random.nextInt(100) < 20) "United Kingdom" else null
          templateJsonStr(id = s"RECORD$id", name = name, town = town, country = country)
        }
      }
    }

    val jsonRecords = genRecords(
      "Daniel",
      "James",
      "Edward",
      "Andrew",
      "Jennifer",
      "Jennifer 8 Lee",
      "",
      null,
      "The corporate CEO of Mitsubishi",
      "130 Jermyn Street, London, SW1Y 4UR",
      "078457598274",
      "Foobar",
      "Sarah",
      "Lizzie",
      "Elizabeth",
      "Lizzie   ",
      "  Daniel     "
    )

    logger.debug("jsonRecords size: {}", jsonRecords.size)

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
        rules = FieldBasedMask(profilers = profilers: _*)
      ).executeMulti().head._2

    profiledRecords.getProfiledData.dataFrame.get.show(false)

    profiledRecords.getMetrics()
  }


  def output(label: String, report: BasicReport)(implicit reportValues: BasicReportTemplate): Unit = {
    val tmpPdfFile = Files.createTempFile(s"metrics-report-$label", ".pdf").toFile
    val htmlReport = report.generateHTML(reportValues)

    val tmpHtmlFile = Files.createTempFile(s"metrics-report-$label", ".html").toFile
    val htmlFw = new FileWriter(tmpHtmlFile)
    htmlFw.write(htmlReport)
    htmlFw.close()
    println(s"Generated HTML report ($label) at: ${tmpHtmlFile.getAbsolutePath}")

    PDFGenerator.generateToFile(htmlReport, tmpPdfFile)
    println(s"Generated PDF report ($label) for Metrics at: ${tmpPdfFile.getAbsolutePath}")

    val pdfBytes = PDFGenerator.generateToByteArray(htmlReport)
    val tmpPdfFile2 = Files.createTempFile(s"metrics-report-$label-viaBytes", ".pdf").toFile
    val fw = new FileOutputStream(tmpPdfFile2)
    fw.write(pdfBytes)
    fw.close()
    println(s"Generated PDF report ($label) (via bytes) for Metrics at: ${tmpPdfFile2.getAbsolutePath}")
  }


  it("outputs a report using a MetricsDataFrame as the source data") {

    println("Metrics Data Frame:")
    metricsDataFrame.dataFrame.get.show(numRows = 100)

    val reportValues = BasicReportTemplate(
      title = "Metrics Test Report",
      date = LocalDate.now().toString,
      sources = Array(),
      rows = MetricsReport.generateReportTemplate(metricsDataFrame).get.toArray
    )

    def output(label: String, report: BasicReport): Unit = {
      val tmpPdfFile = Files.createTempFile(s"metrics-report-$label", ".pdf").toFile
      val htmlReport = report.generateHTML(reportValues)
      PDFGenerator.generateToFile(htmlReport, tmpPdfFile)
      println(s"Generated PDF report ($label) for Metrics at: ${tmpPdfFile.getAbsolutePath}")

      val pdfBytes = PDFGenerator.generateToByteArray(htmlReport)
      val tmpPdfFile2 = Files.createTempFile(s"metrics-report-$label-viaBytes", ".pdf").toFile
      val fw = new FileOutputStream(tmpPdfFile2)
      fw.write(pdfBytes)
      fw.close()
      println(s"Generated PDF report ($label) (via bytes) for Metrics at: ${tmpPdfFile2.getAbsolutePath}")
    }

    output("BasicReport", BasicReport())
  }

  it("outputs a metrics report on companies house sample data") {
    val recordSets = RecordSets(
      "input" -> FlattenedRecords(CSVInputReader.readCSV(spark,
        "src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv",
        idColumn = "CompanyName"))
    )

    val results =
      ProfilerConfiguration(
        recordSets = recordSets,
        rules = FieldBasedMask(profilers = profilers: _*)
      ).executeMulti().head._2

    val metrics = results.getMetrics()

    implicit val reportValues: BasicReportTemplate = BasicReportTemplate(
      title = "Metrics Test Report",
      date = LocalDate.now().toString,
      sources = Array(),
      rows = MetricsReport.generateReportTemplate(metrics).get.toArray
    )

    output("BasicReport", BasicReport())
  }

  it("outputs a metrics report on full companies house data") {

    val fullCompaniesHouse = "src/test/resources/test-data/BasicCompanyDataAsOneFile-2019-08-01.csv.bz2"

    if(new File(fullCompaniesHouse).exists()) {

      val recordSets = RecordSets(
        "input" -> FlattenedRecords(CSVInputReader.readCSV(spark,
          path = fullCompaniesHouse,
          idColumn = "CompanyName"))
      )

      val results =
        ProfilerConfiguration(
          recordSets = recordSets,
          rules = FieldBasedMask(profilers = profilers: _*)
        ).executeMulti().head._2

      val metrics = results.getMetrics()

      implicit val reportValues: BasicReportTemplate = BasicReportTemplate(
        title = "Metrics Test Report",
        date = LocalDate.now().toString,
        sources = Array(),
        rows = MetricsReport.generateReportTemplate(metrics).get.toArray
      )

      output("BasicReport", BasicReport())
    }
  }

}
