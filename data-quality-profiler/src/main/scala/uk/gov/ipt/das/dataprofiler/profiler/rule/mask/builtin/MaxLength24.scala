package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMaxLength

object MaxLength24 {
  def apply(): BuiltIn =
    StringMaxLength(
      name = "MaxLength24",
      maxLength = 24,
      nullAllowed = true
    )
}