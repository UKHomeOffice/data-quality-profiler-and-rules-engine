package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMinLength

object MinLength2 {
  def apply(): BuiltIn =
    StringMinLength(
      name = "MinLength2",
      minLength = 2,
      nullAllowed = true
    )
}