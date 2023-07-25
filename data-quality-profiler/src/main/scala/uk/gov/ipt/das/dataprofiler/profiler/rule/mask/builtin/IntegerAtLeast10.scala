package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.IntegerMinValue

object IntegerAtLeast10 {
  def apply(): BuiltIn =
    IntegerMinValue(
      name = "IntegerAtLeast10",
      minValue = 10,
      nullAllowed = false
    )
}