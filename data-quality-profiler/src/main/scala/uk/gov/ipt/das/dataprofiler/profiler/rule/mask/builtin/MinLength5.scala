package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMinLength

object MinLength5 {
  def apply(): BuiltIn =
    StringMinLength(
      name = "MinLength5",
      minLength = 5,
      nullAllowed = true
    )
}