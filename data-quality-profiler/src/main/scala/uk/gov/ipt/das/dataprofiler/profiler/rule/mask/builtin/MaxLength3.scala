package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMaxLength

object MaxLength3 {
  def apply(): BuiltIn =
    StringMaxLength(
      name = "MaxLength3",
      maxLength = 3,
      nullAllowed = true
    )
}