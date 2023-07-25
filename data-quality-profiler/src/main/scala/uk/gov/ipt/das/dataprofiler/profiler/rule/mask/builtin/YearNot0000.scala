package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringNotMatch

object YearNot0000 {
  def apply(): BuiltIn =
    StringNotMatch(
      name = "YearNot0000",
      notMatch = "0000",
      nullAllowed = false
    )
}