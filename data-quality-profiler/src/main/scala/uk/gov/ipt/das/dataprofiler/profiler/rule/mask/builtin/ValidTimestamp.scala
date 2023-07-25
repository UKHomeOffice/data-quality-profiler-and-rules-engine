package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMatchesPattern

import java.util.regex.Pattern

object ValidTimestamp {
  def apply(): BuiltIn =
    StringMatchesPattern(
      name = "ValidTimestamp",
      pattern = validTimestampPattern,
      nullAllowed = false
    )

  private val validTimestampPattern: Pattern = Pattern.compile("^([01]\\d|2[0-3]):?([0-5]\\d)$")
}