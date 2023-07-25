package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMinLength

object MinLength1 {
  def apply(): BuiltIn =
    StringMinLength(
      name = "MinLength1",
      minLength = 1,
      nullAllowed = true
    )
}