package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringDateTimeAfter

import java.time.{OffsetDateTime, ZoneOffset}

object DateTimeAfter19900101 {
  def apply(): BuiltIn =
    StringDateTimeAfter(
      name = "DateTimeAfter19900101",
      afterDate = date19000101,
      nullAllowed = false
    )

  private val date19000101: OffsetDateTime = OffsetDateTime.of(1990,1,1,0,0,0,0,ZoneOffset.UTC)
}