package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringDateAfter

import java.time.{LocalDate, OffsetDateTime, ZoneOffset}

object DateAfter19900101 {
  def apply(): BuiltIn =
    StringDateAfter(
      name = "DateAfter19900101",
      afterDate = date19000101,
      nullAllowed = false
    )

  private val date19000101: LocalDate = LocalDate.of(1990,1,1 )
}