package uk.gov.ipt.das.dataprofiler.reporting.template

import com.github.mustachejava.DefaultMustacheFactory
import org.apache.commons.io.IOUtils
import BasicReport.mf
import ProfileValue.ZERO_WIDTH_SPACE
import uk.gov.ipt.das.dataprofiler.reporting.template.model.BasicReportTemplate

import java.io.{StringReader, StringWriter}
import java.nio.charset.StandardCharsets
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class BasicReport private (templateFilename: String) {

  private val templateString = IOUtils.toString(
    getClass.getClassLoader.getResource(templateFilename).openStream(), StandardCharsets.UTF_8)

  def additionalHTMLEscape(s: String): String =
    s.replaceAll(ZERO_WIDTH_SPACE, "<wbr>")

  def generateHTML(reportValues: BasicReportTemplate): String = {
    val writer = new StringWriter()
    val mustache = mf.compile(new StringReader(templateString), templateFilename)
    mustache.execute(writer, reportValues)
    additionalHTMLEscape(writer.toString)
  }

  def generateRedactedHTML(reportValues: BasicReportTemplate): String =
    generateHTML(reportValues.redacted)

}
object BasicReport {
  private val mf = new DefaultMustacheFactory
  def apply(): BasicReport = new BasicReport("html-report/basic-report.html.mustache")
}