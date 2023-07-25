package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMaxLength

object MaxLength90 {
  def apply(): BuiltIn =
    StringMaxLength(
      name = "MaxLength90",
      maxLength = 90,
      nullAllowed = true
    )
}