package uk.gov.ipt.das.dataprofiler.reporting.template.model

import uk.gov.ipt.das.dataprofiler.reporting.template.{NameValuePair, ProfileValue}
import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.SplitLongWords

case class BasicReportRowTemplate private (json_path: String,
                                           percent_populated: String,
                                           summary: Array[NameValuePair],
                                           assertionRuleCompliance: Array[AssertionRuleCompliance],
                                           highGrainHistogram: String,
                                           lowGrainHistogram: String,
                                           hg_count: String,
                                           lg_count: String,
                                           top10_hg: Array[ProfileValue],
                                           bottom10_hg: Array[ProfileValue],
                                           top10_lg: Array[ProfileValue],
                                           bottom10_lg: Array[ProfileValue])

object BasicReportRowTemplate extends SplitLongWords {

  // TODO can the below be unified into a single call?

  private def removeLastComma(a: Array[NameValuePair]): Array[NameValuePair] =
    a.zipWithIndex.map{ case (f, i) => if (i == a.length - 1) f.withNoComma else f }

  private def removeLastComma(a: Array[ProfileValue]): Array[ProfileValue] =
    a.zipWithIndex.map { case (f, i) => if (i == a.length - 1) f.withNoComma else f }

  def apply(json_path: String,
            percent_populated: String,
            summary: Array[NameValuePair],
            assertionRuleCompliance: Array[AssertionRuleCompliance],
            highGrainHistogram: String,
            lowGrainHistogram: String,
            hg_count: String,
            lg_count: String,
            top10_hg: Array[ProfileValue],
            bottom10_hg: Array[ProfileValue],
            top10_lg: Array[ProfileValue],
            bottom10_lg: Array[ProfileValue]): BasicReportRowTemplate =
    new BasicReportRowTemplate(
      json_path = slwPath(json_path),
      percent_populated = percent_populated,
      highGrainHistogram = highGrainHistogram,
      lowGrainHistogram = lowGrainHistogram,
      hg_count = hg_count,
      lg_count = lg_count,
      summary = removeLastComma(summary),
      assertionRuleCompliance = assertionRuleCompliance,
      top10_hg = removeLastComma(top10_hg),
      bottom10_hg = removeLastComma(bottom10_hg),
      top10_lg = removeLastComma(top10_lg),
      bottom10_lg = removeLastComma(bottom10_lg)
    )
}