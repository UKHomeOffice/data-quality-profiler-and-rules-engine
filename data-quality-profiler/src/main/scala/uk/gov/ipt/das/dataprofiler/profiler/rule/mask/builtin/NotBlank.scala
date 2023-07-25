package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringNotBlank

object NotBlank {
  def apply(): BuiltIn =
    StringNotBlank(
      name = "NotBlank",
      nullAllowed = false
    )
}