package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringNotMatch

object NotMinusOne {
  def apply(): BuiltIn =
    StringNotMatch(
      name = "NotMinusOne",
      notMatch = "-1",
      nullAllowed = true
    )
}