package uk.gov.ipt.das.dataprofiler.reporting.template

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.reporting.template.model.{BasicReportRowTemplate, BasicReportTemplate}
import uk.gov.ipt.das.reflection.CaseClassToMap

import java.io.{File, FileWriter}
import java.time.OffsetDateTime

class BasicReportTest extends AnyFunSpec with CaseClassToMap {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  it("generates a basic report from test data") {
    val reportValues = BasicReportTemplate(
      title = "Test Report",
      date = OffsetDateTime.now().toLocalDate.toString,
      sources = Array(),
      rows = Array(
        BasicReportRowTemplate(
          json_path = "some.path.for.the.tests",
          percent_populated = "88.8",
          lowGrainHistogram = "",
          highGrainHistogram = "",
          hg_count = "",
          lg_count = "",
          summary = Array(
            NameValuePair(name = "min", value = "aardvark"),
            NameValuePair(name = "max", value = "zebra"),
            NameValuePair(name = "count", value = "88")
          ),
          assertionRuleCompliance = Array(),
          top10_hg = Array(ProfileValue("Aaaaa", "Smith", "10"), ProfileValue("Aaaa", "Lodz", "9"), ProfileValue("Aaaaaaa", "Crentist", "8"), ProfileValue("Aaaaaa", "Foobar", "7")),
          bottom10_hg = Array(ProfileValue("Aaaaaa Aaaa aa Aaa Aaaaaaaa Aaaaa","Prince Hans of the Southern Isles", "5"),
            ProfileValue("Aaa Aaaaaaaa AAA aa Aaaaaaaaaa Aaaa", "The Corporate CEO of Mitsubishi Corp)", "")),
          top10_lg = Array(ProfileValue("Aa","Smith","11"), ProfileValue("A9","Smith0","10"), ProfileValue("A A", "Mr Smith","9")),
          bottom10_lg = Array(ProfileValue("Aaaaaa Aaaa aa Aaa Aaaaaaaa Aaaaa","Prince Hans of the Southern Isles", "5"))
        ),
        BasicReportRowTemplate(
          json_path = "another.path.for.the.tests",
          percent_populated = "82.1",
          lowGrainHistogram = "",
          highGrainHistogram = "",
          hg_count = "",
          lg_count = "",
          summary = Array(
            NameValuePair(name = "min", value = "acrobat"),
            NameValuePair(name = "max", value = "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"),
            NameValuePair(name = "count", value = "82")
          ),
          assertionRuleCompliance = Array(),
          top10_hg = Array(ProfileValue("Aaaaa", "Smith", "10"), ProfileValue("Aaaa", "Lodz", "9"), ProfileValue("Aaaaaaa", "Crentist", "8"), ProfileValue("Aaaaaa", "Foobar", "7")),
          bottom10_hg = Array(ProfileValue("Aaaaaa Aaaa aa Aaa Aaaaaaaa Aaaaa", "Prince Hans of the Southern Isles", "5"),
            ProfileValue("Aaa Aaaaaaaa AAA aa Aaaaaaaaaa Aaaa", "The Corporate CEO of Mitsubishi Corp)", "")),
          top10_lg = Array(ProfileValue("Aa", "Smith", "11"), ProfileValue("A9", "Smith0", "10"), ProfileValue("A A", "Mr Smith", "9")),
          bottom10_lg = Array(ProfileValue("Aaaaaa Aaaa aa Aaa Aaaaaaaa Aaaaa", "Prince Hans of the Southern Isles", "5"))
          )
      )
    )

    val report = BasicReport()

    // this is for debugging only
    val variables = caseClassToJavaMap(reportValues)
    logger.debug("BasicReportTest: \"generates a basic report from test data\", variables: {}", variables)

    val htmlReport = report.generateHTML(reportValues)

    val tmpHtmlFile = File.createTempFile("basic-report", ".html")
    println(htmlReport)

    val fw = new FileWriter(tmpHtmlFile)
    fw.write(htmlReport)
    fw.close()

    println(s"HTML report written to: ${tmpHtmlFile.getAbsolutePath}")

    val tmpPdfFile = File.createTempFile("basic-report", ".pdf")
    PDFGenerator.generateToFile(htmlReport, tmpPdfFile)
    println(s"PDF file output to ${tmpPdfFile.getAbsolutePath}")
  }

}
