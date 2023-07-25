package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringDateAfter

import java.time.LocalDate

object DateAfter19850101 {
  def apply(): BuiltIn =
    StringDateAfter(
      name = "DateAfter19850101",
      afterDate = date19850101,
      nullAllowed = false
    )

  private val date19850101: LocalDate = LocalDate.of(1985,1,1 )
}