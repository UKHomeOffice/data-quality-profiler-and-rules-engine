package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMinLength

object MinLength3 {
  def apply(): BuiltIn =
    StringMinLength(
      name = "MinLength3",
      minLength = 3,
      nullAllowed = true
    )
}