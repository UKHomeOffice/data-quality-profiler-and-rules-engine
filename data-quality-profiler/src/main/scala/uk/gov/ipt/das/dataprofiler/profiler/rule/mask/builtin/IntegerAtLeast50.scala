package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.IntegerMinValue

object IntegerAtLeast50 {
  def apply(): BuiltIn =
    IntegerMinValue(
      name = "IntegerAtLeast50",
      minValue = 50,
      nullAllowed = false
    )
}