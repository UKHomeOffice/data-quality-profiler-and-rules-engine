package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringExactLength

object Length3 {
  def apply(): BuiltIn =
    StringExactLength(
      name = "Length3",
      exactLength = 3,
      nullAllowed = true
    )
}