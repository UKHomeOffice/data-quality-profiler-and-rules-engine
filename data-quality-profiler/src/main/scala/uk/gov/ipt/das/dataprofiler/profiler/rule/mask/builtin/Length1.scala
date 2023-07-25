package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringExactLength

object Length1 {
  def apply(): BuiltIn =
    StringExactLength(
      name = "Length1",
      exactLength = 1,
      nullAllowed = true
    )
}