package uk.gov.ipt.das.dataprofiler.reporting.template

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.reporting.template.model.{BasicReportRowTemplate, BasicReportTemplate}

class RedactSamplesTest extends AnyFunSpec {

  private val sampleString = "SENSITIVE DATA"

  it("redacts samples from report output") {
    val rows = Array(BasicReportRowTemplate(
      json_path = "some.path.for.the.test",
      percent_populated = "100.0",
      summary = Array(NameValuePair("Count", "100")),
      assertionRuleCompliance = Array(),
      highGrainHistogram = "",
      lowGrainHistogram = "",
      hg_count = "",
      lg_count = "",
      top10_hg = Array(ProfileValue("AAA", sampleString, "50")),
      bottom10_hg = Array(ProfileValue("AAA", sampleString, "50")),
      top10_lg = Array(ProfileValue("AAA", sampleString, "50")),
      bottom10_lg = Array(ProfileValue("AAA", sampleString, "50"))
    ))

    val normalData = BasicReportTemplate(
      title = "Full data report", date = "2022-09-14", sources = Array(), rows = rows)

    val redactedData = BasicReportTemplate(
      title = "Full data report", date = "2022-09-14", sources = Array(), rows = rows)(redactSamples = true)

    val withSensitiveData = BasicReport().generateHTML(normalData)
    val noSensitiveData = BasicReport().generateHTML(redactedData)
    val noSensitiveData2 = BasicReport().generateRedactedHTML(normalData)

    assert(withSensitiveData.contains(sampleString))
    assert(!noSensitiveData.contains(sampleString))
    assert(!noSensitiveData2.contains(sampleString))
  }

}
