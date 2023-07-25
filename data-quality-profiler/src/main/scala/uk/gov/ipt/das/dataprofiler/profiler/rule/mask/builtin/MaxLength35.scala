package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMaxLength

object MaxLength35 {
  def apply(): BuiltIn =
    StringMaxLength(
      name = "MaxLength35",
      maxLength = 35,
      nullAllowed = true
    )
}