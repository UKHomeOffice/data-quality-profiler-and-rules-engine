package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMaxLength

object MaxLength9 {
  def apply(): BuiltIn =
    StringMaxLength(
      name = "MaxLength9",
      maxLength = 9,
      nullAllowed = true
    )
}