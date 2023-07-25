package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMatchesPattern

import java.util.regex.Pattern

object ValidYear {
  def apply(): BuiltIn =
    StringMatchesPattern(
      name = "ValidYear",
      pattern = validYearPattern,
      nullAllowed = false
    )

  private val validYearPattern: Pattern = Pattern.compile("^[12][0-9]{3}$")
}